package com.smartship.edge.routing.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.observability.SmartShipMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 耐久写入语义：瞬态失败一次重试吸收；持续失败转本地兜底表并计数，绝不静默丢失。
 */
class PersistenceFallbackTest {

    private SimpleMeterRegistry registry;
    private SmartShipMetrics metrics;
    private EdgeProperties properties;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new SmartShipMetrics(registry);
        properties = new EdgeProperties();
        properties.setMmsi("413999999");
        properties.setSchemaReady(true);
        properties.getCollect().getPersist().setEnabled(true);
    }

    private Counter counter(String name, String... tags) {
        Counter c = registry.find(name).tags(tags).counter();
        assertNotNull(c, "指标必须已注册: " + name);
        return c;
    }

    @Test
    @DisplayName("瞬态失败一次：重试成功，不计失败不进兜底")
    void transientFailureRecoveredByRetry() {
        JdbcTemplate jt = mock(JdbcTemplate.class);
        when(jt.update(anyString(), any(Object[].class)))
                .thenThrow(new RuntimeException("glitch"))
                .thenReturn(1);

        NmeaDataPersistenceService service =
                new NmeaDataPersistenceService(jt, properties, null, metrics);
        service.saveGps("RMC", "SERIAL", 31.2, 121.5, 12.0, 180.0,
                180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A", "413999999");

        assertEquals(1.0, counter("smartship_persistence_writes_total",
                "type", "gps", "result", "success").count());
        assertEquals(0, registry.find("smartship_persistence_writes_total")
                .tags("type", "gps", "result", "failure").counters().size()
                + registry.find("smartship_persistence_fallback_total").counters().size(),
                "重试成功不得产生失败/兜底计数");
    }

    @Test
    @DisplayName("持续失败：主表两次+兜底一次，失败与兜底各计一次")
    void persistentFailureGoesToFallback() {
        JdbcTemplate jt = mock(JdbcTemplate.class);
        when(jt.update(anyString(), any(Object[].class)))
                .thenThrow(new RuntimeException("DB down"))
                .thenThrow(new RuntimeException("DB down"))
                .thenReturn(1);

        NmeaDataPersistenceService service =
                new NmeaDataPersistenceService(jt, properties, null, metrics);
        service.saveWind("MWV", "SERIAL", 10.0, 5.0, 20.0, 21.0, 6.0, "413999999");

        verify(jt, times(3)).update(anyString(), any(Object[].class));
        assertEquals(1.0, counter("smartship_persistence_writes_total",
                "type", "wind", "result", "failure").count());
        assertEquals(1.0, counter("smartship_persistence_fallback_total",
                "type", "wind").count());

        // 第三次调用是兜底表插入：payload 必须是可回放 JSON（与回放器同一编解码）。
        org.mockito.ArgumentCaptor<String> sqlCaptor =
                org.mockito.ArgumentCaptor.forClass(String.class);
        org.mockito.ArgumentCaptor<Object[]> argsCaptor =
                org.mockito.ArgumentCaptor.forClass(Object[].class);
        verify(jt, times(3)).update(sqlCaptor.capture(), argsCaptor.capture());
        Object[] fallbackArgs = argsCaptor.getAllValues().get(2);
        assertEquals("wind", fallbackArgs[0]);
        assertTrue(fallbackArgs[2] != null && !String.valueOf(fallbackArgs[2]).isBlank(),
                "兜底行必须携带统一 replay_id");
        com.smartship.edge.persist.FileFallbackStore.ParsedLine parsed =
                com.smartship.edge.persist.FileFallbackStore.parseLine((String) fallbackArgs[3]);
        assertEquals("wind", parsed.stream());
        assertEquals("413999999", parsed.mmsi());
        assertEquals(10, parsed.args().size(), "wind 行 10 个参数必须完整可回放");
    }

    @Test
    @DisplayName("真库兜底行可查：持续失败后 failed_writes 落一笔")
    void fallbackRowLandsInRealTable() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:fallback_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL",
                "sa", "");
        JdbcTemplate jt = new JdbcTemplate(ds);
        jt.execute("CREATE TABLE zncb_depth_data (id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + " ship_id VARCHAR(64), mmsi VARCHAR(32), replay_id VARCHAR(64) NULL)");
        // 主表缺列（无 depth_m）→ 每次 update 必抛；兜底表正常 → 精确落一笔
        jt.execute("CREATE TABLE zncb_failed_writes (id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + " stream VARCHAR(32), mmsi VARCHAR(32), replay_id VARCHAR(64) NULL,"
                + " payload TEXT, error VARCHAR(500))");

        NmeaDataPersistenceService service =
                new NmeaDataPersistenceService(jt, properties, null, metrics);
        service.saveDepth("DPT", "SERIAL", 12.5, 0.5, "413999999");

        Long n = jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class);
        assertEquals(1L, n, "兜底表必须落一笔可回放行");
        assertEquals(1.0, counter("smartship_persistence_fallback_total",
                "type", "depth").count());
    }

    @Test
    @DisplayName("批量路径：连接都拿不到时整批进三级兜底，不静默丢失")
    void batchFallbackWhenConnectionUnavailable() throws Exception {
        JdbcTemplate jt = mock(JdbcTemplate.class);
        javax.sql.DataSource ds = mock(javax.sql.DataSource.class);
        when(jt.getDataSource()).thenReturn(ds);
        when(ds.getConnection()).thenThrow(new java.sql.SQLException("MySQL down"));
        when(jt.update(org.mockito.ArgumentMatchers.argThat(
                (String sql) -> sql != null && sql.contains("zncb_failed_writes")),
                any(Object[].class))).thenReturn(1);

        com.smartship.edge.persist.FileFallbackStore store =
                mock(com.smartship.edge.persist.FileFallbackStore.class);
        NmeaDataPersistenceService service =
                new NmeaDataPersistenceService(jt, properties, null, metrics, store);
        java.util.List<EnginePoint> batch = java.util.List.of(
                EnginePoint.now("413999999", 1, 1500.0, 85.0, 0.5, 0.4, 420.0, 0.25,
                        2.8, 75.0, 24.5, 12000, 1, 0, 0),
                EnginePoint.now("413999999", 2, 1500.0, 85.0, 0.5, 0.4, 420.0, 0.25,
                        2.8, 75.0, 24.5, 12000, 1, 0, 0));

        assertEquals(0, service.saveEngineBatch(batch), "主表无一行落库");
        assertEquals(2.0, counter("smartship_persistence_fallback_total",
                "type", "engine").count(), "两行必须全部进入兜底");
        verify(jt, times(2)).update(
                org.mockito.ArgumentMatchers.argThat(
                        (String sql) -> sql != null && sql.contains("zncb_failed_writes")),
                any(Object[].class));
    }

    @Test
    @DisplayName("批量路径：逐行补写两次失败的行进三级兜底")
    void batchRowFallbackAfterRetryExhausted() throws Exception {
        JdbcTemplate jt = mock(JdbcTemplate.class);
        javax.sql.DataSource ds = mock(javax.sql.DataSource.class);
        when(jt.getDataSource()).thenReturn(ds);
        java.sql.Connection con = mock(java.sql.Connection.class);
        when(ds.getConnection()).thenReturn(con);
        java.sql.PreparedStatement ps = mock(java.sql.PreparedStatement.class);
        when(con.prepareStatement(anyString())).thenReturn(ps);
        // 整批执行返回 EXECUTE_FAILED → 回滚 → 逐行补写
        when(ps.executeBatch()).thenReturn(new int[]{java.sql.Statement.EXECUTE_FAILED});
        // 逐行补写全部抛，兜底插入成功
        when(jt.update(anyString(), any(Object[].class))).thenAnswer(inv -> {
            String sql = inv.getArgument(0);
            if (sql.contains("zncb_failed_writes")) {
                return 1;
            }
            throw new RuntimeException("row down");
        });

        com.smartship.edge.persist.FileFallbackStore store =
                mock(com.smartship.edge.persist.FileFallbackStore.class);
        NmeaDataPersistenceService service =
                new NmeaDataPersistenceService(jt, properties, null, metrics, store);
        java.util.List<EnginePoint> batch = java.util.List.of(
                EnginePoint.now("413999999", 1, 1500.0, 85.0, 0.5, 0.4, 420.0, 0.25,
                        2.8, 75.0, 24.5, 12000, 1, 0, 0));

        assertEquals(0, service.saveEngineBatch(batch));
        assertEquals(1.0, counter("smartship_persistence_fallback_total",
                "type", "engine").count(), "补写耗尽的行必须进兜底");
    }

    @Test
    @DisplayName("基础SQL自带replay_id列：5流全覆盖（回放复用同一语句）")
    void baseSqlCarriesReplayIdForAllStreams() {
        for (String stream : new String[]{"gps", "wind", "depth", "rudder", "engine"}) {
            String sql = NmeaDataPersistenceService.sqlForStream(stream);
            assertNotNull(sql, stream);
            assertTrue(sql.contains(", replay_id)"),
                    stream + " INSERT 必须包含 replay_id 列");
            assertTrue(sql.contains("INSERT INTO zncb_" + stream),
                    stream + " 表名必须正确");
        }
        assertNull(NmeaDataPersistenceService.sqlForStream("nope"));
    }

    private static final String ENGINE_DDL = """
            CREATE TABLE zncb_engine_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ship_id VARCHAR(64), mmsi VARCHAR(32), slave_id INT,
                protocol VARCHAR(20) DEFAULT 'MODBUS_TCP', timestamp DATETIME,
                rpm DOUBLE, coolant_temp DOUBLE, lube_oil_press DOUBLE,
                fuel_press DOUBLE, exhaust_temp DOUBLE, tc_air_press DOUBLE,
                start_air_press DOUBLE, bearing_temp DOUBLE, battery_volt DOUBLE,
                running_hours INT, status INT, alarm_bits1 INT, alarm_bits2 INT,
                replay_id VARCHAR(64) NULL,
                CONSTRAINT uk_engine_replay_id UNIQUE (replay_id)
            )""";

    private static final String GPS_DDL = """
            CREATE TABLE zncb_gps_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ship_id VARCHAR(64), mmsi VARCHAR(32), sentence_type VARCHAR(16),
                source VARCHAR(32), timestamp DATETIME,
                latitude DOUBLE, longitude DOUBLE, speed_knots DOUBLE,
                course_over_ground DOUBLE, heading_true DOUBLE, heading_magnetic DOUBLE,
                magnetic_variation DOUBLE, altitude_m DOUBLE, satellites INT,
                hdop DOUBLE, position_quality INT, gps_status VARCHAR(16),
                replay_id VARCHAR(64) NULL,
                CONSTRAINT uk_gps_replay_id UNIQUE (replay_id)
            )""";

    private JdbcTemplate h2(String prefix, String... ddls) {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:" + prefix + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL",
                "sa", "");
        JdbcTemplate jt = new JdbcTemplate(ds);
        for (String ddl : ddls) {
            jt.execute(ddl);
        }
        return jt;
    }

    /**
     * 模拟“已提交但返回异常”：第一次真实执行后抛异常，之后透传。
     * 只对匹配的 SQL 生效，其他语句直接透传。
     */
    static class CommitUnknownJdbcTemplate extends JdbcTemplate {
        private final java.util.concurrent.atomic.AtomicBoolean armed =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        private final java.util.function.Predicate<String> match;

        CommitUnknownJdbcTemplate(javax.sql.DataSource ds, java.util.function.Predicate<String> match) {
            super(ds);
            this.match = match;
        }

        @Override
        public int update(String sql, Object... args) throws DataAccessException {
            if (armed.compareAndSet(true, false) && match.test(sql)) {
                int n = super.update(sql, args);
                assertEquals(1, n);
                throw new UncategorizedSQLException("commit-unknown", sql,
                        new SQLException("response lost after commit"));
            }
            return super.update(sql, args);
        }
    }

    @Test
    @DisplayName("规格a：主表第一次已提交但返回异常，重试去重吸收后仍1条")
    void mainUnknownResultAbsorbedOnRetry() throws Exception {
        JdbcTemplate real = h2("speca_", GPS_DDL);
        CommitUnknownJdbcTemplate flaky = new CommitUnknownJdbcTemplate(
                real.getDataSource(), sql -> sql.contains("zncb_gps_data"));
        com.smartship.edge.persist.FileFallbackStore store =
                mock(com.smartship.edge.persist.FileFallbackStore.class);

        NmeaDataPersistenceService service =
                new NmeaDataPersistenceService(flaky, properties, null, metrics, store);
        service.saveGps("RMC", "SERIAL", 31.2, 121.5, 12.0, 180.0,
                180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A", "413999999");

        assertEquals(1L, real.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class),
                "未知结果重试必须去重，主表仍1条");
        assertEquals(1.0, counter("smartship_persistence_writes_total",
                "type", "gps", "result", "success").count());
        org.mockito.Mockito.verify(store, org.mockito.Mockito.never())
                .spool(anyString(), anyString(), anyString(), any(Object[].class));
    }

    @Test
    @DisplayName("规格b：兜底表已插入但返回异常，不产生第二份逻辑数据")
    void fallbackUnknownResultDoesNotDuplicate() throws Exception {
        JdbcTemplate real = h2("specb_", GPS_DDL,
                "CREATE TABLE zncb_failed_writes (id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                        + " stream VARCHAR(32), mmsi VARCHAR(32), replay_id VARCHAR(64) NULL,"
                        + " payload TEXT, error VARCHAR(500),"
                        + " CONSTRAINT uk_failed_replay_id UNIQUE (replay_id))");
        JdbcTemplate flaky = new JdbcTemplate(real.getDataSource()) {
            private boolean fbArmed = true;

            @Override
            public int update(String sql, Object... args) throws DataAccessException {
                if (sql.contains("zncb_gps_data")) {
                    throw new RuntimeException("main down");
                }
                if (sql.contains("zncb_failed_writes") && fbArmed) {
                    fbArmed = false;
                    int n = super.update(sql, args);
                    assertEquals(1, n);
                    throw new UncategorizedSQLException("commit-unknown", sql,
                            new SQLException("response lost after commit"));
                }
                return super.update(sql, args);
            }
        };
        com.smartship.edge.persist.FileFallbackStore store =
                mock(com.smartship.edge.persist.FileFallbackStore.class);

        NmeaDataPersistenceService service =
                new NmeaDataPersistenceService(flaky, properties, null, metrics, store);
        service.saveDepth("DPT", "SERIAL", 12.5, 0.5, "413999999");

        assertEquals(1L, real.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "兜底重试命中唯一约束，不产生第二份逻辑数据");
        assertEquals(1.0, counter("smartship_persistence_fallback_total",
                "type", "depth").count());
        org.mockito.Mockito.verify(store, org.mockito.Mockito.never())
                .spool(anyString(), anyString(), anyString(), any(Object[].class));
    }

    @Test
    @DisplayName("规格d：Batch内同键冲突→回滚→逐行补写去重吸收，最终仍1条")
    void batchRowMultiWriteStaysSingleRow() throws Exception {
        DriverManagerDataSource realDs = new DriverManagerDataSource(
                "jdbc:h2:mem:specd_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL",
                "sa", "");
        new JdbcTemplate(realDs).execute(ENGINE_DDL);
        com.smartship.edge.persist.FileFallbackStore store =
                mock(com.smartship.edge.persist.FileFallbackStore.class);

        // 同一批两行共享 replay_id：整批第二行违反唯一约束→回滚→逐行补写→去重吸收。
        // 全程真连接，覆盖 batch→row→absorb 完整路径（mock 连接会污染行补写的 DataSource）。
        JdbcTemplate realJt = new JdbcTemplate(realDs);
        NmeaDataPersistenceService service =
                new NmeaDataPersistenceService(realJt, properties, null, metrics, store);
        java.time.LocalDateTime ts = java.time.LocalDateTime.of(2026, 9, 19, 10, 0, 0);
        List<EnginePoint> batch = List.of(
                new EnginePoint("413999999", 1, 1500.0, 85.0, 0.5, 0.4, 420.0, 0.25,
                        2.8, 75.0, 24.5, 12000, 1, 0, 0, ts, "shared-batch-key-d"),
                new EnginePoint("413999999", 1, 1500.0, 85.0, 0.5, 0.4, 420.0, 0.25,
                        2.8, 75.0, 24.5, 12000, 1, 0, 0, ts, "shared-batch-key-d"));

        assertEquals(2, service.saveEngineBatch(batch), "两行补写都算成功");
        JdbcTemplate check = new JdbcTemplate(realDs);
        assertEquals(1L, check.queryForObject("SELECT COUNT(*) FROM zncb_engine_data", Long.class),
                "同键批量写入最终仍1条");
        org.mockito.Mockito.verify(store, org.mockito.Mockito.never())
                .spool(anyString(), anyString(), anyString(), any(Object[].class));
    }
}
