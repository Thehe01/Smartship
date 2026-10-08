package com.smartship.edge.routing.service;

import com.smartship.edge.persistence.MyBatisTestSupport;
import com.smartship.edge.persistence.EdgeTelemetryRepository;
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
        EdgeTelemetryRepository repository = mock(EdgeTelemetryRepository.class);
        when(repository.insert(anyString(), any(Object[].class)))
                .thenThrow(new RuntimeException("glitch")).thenReturn(1);
        NmeaDataPersistenceService service = new NmeaDataPersistenceService(repository, properties, null, metrics);
        saveGps(service);
        assertEquals(1.0, counter("smartship_persistence_writes_total",
                "type", "gps", "result", "success").count());
        org.mockito.Mockito.verify(repository, org.mockito.Mockito.never()).insertFallback(any());
        assertTrue(registry.find("smartship_persistence_fallback_total").counters().isEmpty());
    }

    @Test
    @DisplayName("持续失败：主表两次后兜底，原参数和replay_id完整保留")
    void persistentFailureGoesToFallback() {
        EdgeTelemetryRepository repository = mock(EdgeTelemetryRepository.class);
        when(repository.insert(anyString(), any(Object[].class))).thenThrow(new RuntimeException("DB down"));
        when(repository.insertFallback(any(Object[].class))).thenReturn(1);
        NmeaDataPersistenceService service = new NmeaDataPersistenceService(repository, properties, null, metrics);
        service.saveWind("MWV", "SERIAL", 10.0, 5.0, 20.0, 21.0, 6.0, "413999999");
        verify(repository, times(2)).insert(anyString(), any(Object[].class));
        var captor = org.mockito.ArgumentCaptor.forClass(Object[].class);
        verify(repository).insertFallback(captor.capture());
        Object[] fallbackArgs = captor.getValue();
        assertEquals("wind", fallbackArgs[0]);
        assertTrue(fallbackArgs[2] instanceof String id && !id.isBlank());
        var parsed = com.smartship.edge.persist.FileFallbackStore.parseLine((String) fallbackArgs[3]);
        assertEquals("wind", parsed.stream());
        assertEquals("413999999", parsed.mmsi());
        assertEquals(fallbackArgs[2], parsed.replayId());
        assertEquals(10, parsed.args().size());
        assertEquals(1.0, counter("smartship_persistence_writes_total", "type", "wind", "result", "failure").count());
        assertEquals(1.0, counter("smartship_persistence_fallback_total", "type", "wind").count());
    }

    @Test
    @DisplayName("真库兜底行可查：主表缺列后failed_writes精确落一笔")
    void fallbackRowLandsInRealTable() {
        JdbcTemplate jdbc = h2("fallback_",
                "CREATE TABLE zncb_depth_data (id BIGINT AUTO_INCREMENT PRIMARY KEY, ship_id VARCHAR(64), mmsi VARCHAR(32), replay_id VARCHAR(64))",
                "CREATE TABLE zncb_failed_writes (id BIGINT AUTO_INCREMENT PRIMARY KEY, stream VARCHAR(32), mmsi VARCHAR(32), replay_id VARCHAR(64), payload TEXT, error VARCHAR(500))");
        NmeaDataPersistenceService service = new NmeaDataPersistenceService(MyBatisTestSupport.repository(jdbc), properties, null, metrics);
        service.saveDepth("DPT", "SERIAL", 12.5, 0.5, "413999999");
        assertEquals(1L, jdbc.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class));
        assertEquals(1.0, counter("smartship_persistence_fallback_total", "type", "depth").count());
    }

    @Test
    @DisplayName("批量路径：DB连接故障后整批进入耐久兜底")
    void batchFallbackWhenConnectionUnavailable() {
        EdgeTelemetryRepository repository = mock(EdgeTelemetryRepository.class);
        when(repository.insertEngineBatch(org.mockito.ArgumentMatchers.anyList()))
                .thenThrow(new org.springframework.dao.DataAccessResourceFailureException("MySQL down"));
        when(repository.insert(anyString(), any(Object[].class))).thenThrow(new RuntimeException("MySQL down"));
        when(repository.insertFallback(any(Object[].class))).thenReturn(1);
        var store = mock(com.smartship.edge.persist.FileFallbackStore.class);
        var service = new NmeaDataPersistenceService(repository, properties, null, metrics, store);
        assertEquals(0, service.saveEngineBatch(List.of(point(), point())));
        verify(repository, times(2)).insertFallback(any(Object[].class));
        assertEquals(2.0, counter("smartship_persistence_fallback_total", "type", "engine").count());
    }

    @Test
    @DisplayName("批量路径：逐行补写两次失败的行进三级兜底")
    void batchRowFallbackAfterRetryExhausted() {
        EdgeTelemetryRepository repository = mock(EdgeTelemetryRepository.class);
        when(repository.insertEngineBatch(org.mockito.ArgumentMatchers.anyList())).thenThrow(new RuntimeException("batch down"));
        when(repository.insert(anyString(), any(Object[].class))).thenThrow(new RuntimeException("row down"));
        when(repository.insertFallback(any(Object[].class))).thenReturn(1);
        var service = new NmeaDataPersistenceService(repository, properties, null, metrics,
                mock(com.smartship.edge.persist.FileFallbackStore.class));
        assertEquals(0, service.saveEngineBatch(List.of(point())));
        verify(repository, times(2)).insert(anyString(), any(Object[].class));
        verify(repository).insertFallback(any(Object[].class));
    }

    @Test
    void streamAllowlistCoversFiveStreamsAndRejectsUnknown() {
        for (String stream : List.of("gps", "wind", "depth", "rudder", "engine", "engine-batch")) {
            assertTrue(NmeaDataPersistenceService.supportsStream(stream));
        }
        org.junit.jupiter.api.Assertions.assertFalse(NmeaDataPersistenceService.supportsStream("nope"));
        org.junit.jupiter.api.Assertions.assertFalse(NmeaDataPersistenceService.supportsStream(null));
    }

    private EnginePoint point() {
        return EnginePoint.now("413999999", 1, 1500.0, 85.0, 0.5, 0.4, 420.0, 0.25,
                2.8, 75.0, 24.5, 12000, 1, 0, 0);
    }
    private void saveGps(NmeaDataPersistenceService service) {
        service.saveGps("RMC", "SERIAL", 31.2, 121.5, 12.0, 180.0,
                180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A", "413999999");
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

    @Test
    @DisplayName("规格a：主表第一次已提交但返回异常，重试去重吸收后仍1条")
    void mainUnknownResultAbsorbedOnRetry() throws Exception {
        JdbcTemplate real = h2("speca_", GPS_DDL);
        EdgeTelemetryRepository flaky = org.mockito.Mockito.spy(MyBatisTestSupport.repository(real));
        var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
        org.mockito.Mockito.doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            if (armed.getAndSet(false)) throw new RuntimeException("response lost after commit");
            return result;
        }).when(flaky).insert(anyString(), any(Object[].class));
        com.smartship.edge.persist.FileFallbackStore store =
                mock(com.smartship.edge.persist.FileFallbackStore.class);

        NmeaDataPersistenceService service =
                new NmeaDataPersistenceService(MyBatisTestSupport.repository(flaky), properties, null, metrics, store);
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
        EdgeTelemetryRepository flaky = org.mockito.Mockito.spy(MyBatisTestSupport.repository(real));
        org.mockito.Mockito.doThrow(new RuntimeException("main down"))
                .when(flaky).insert(anyString(), any(Object[].class));
        var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
        org.mockito.Mockito.doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            if (armed.getAndSet(false)) throw new RuntimeException("response lost after commit");
            return result;
        }).when(flaky).insertFallback(any(Object[].class));
        com.smartship.edge.persist.FileFallbackStore store =
                mock(com.smartship.edge.persist.FileFallbackStore.class);

        NmeaDataPersistenceService service =
                new NmeaDataPersistenceService(MyBatisTestSupport.repository(flaky), properties, null, metrics, store);
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
                new NmeaDataPersistenceService(MyBatisTestSupport.repository(realJt), properties, null, metrics, store);
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
