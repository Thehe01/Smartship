package com.smartship.edge.persist;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.observability.SmartShipMetrics;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 兜底回放：MySQL 全挂时 spool 落盘，恢复后重放进主表并清文件。
 */
class FallbackReplayTest {

    @TempDir
    Path tempDir;

    private static final String GPS_DDL = """
            CREATE TABLE zncb_gps_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ship_id VARCHAR(64), mmsi VARCHAR(32), sentence_type VARCHAR(16),
                source VARCHAR(32), timestamp DATETIME,
                latitude DOUBLE, longitude DOUBLE, speed_knots DOUBLE,
                course_over_ground DOUBLE, heading_true DOUBLE, heading_magnetic DOUBLE,
                magnetic_variation DOUBLE, altitude_m DOUBLE, satellites INT,
                hdop DOUBLE, position_quality INT, gps_status VARCHAR(16)
            )""";

    private JdbcTemplate realDb() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:replay_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL",
                "sa", "");
        JdbcTemplate jt = new JdbcTemplate(ds);
        jt.execute(GPS_DDL);
        return jt;
    }

    private Object[] gpsArgs() {
        return new Object[]{"S001", "413999999", "RMC", "SERIAL",
                LocalDateTime.of(2026, 9, 19, 10, 0, 0),
                31.2, 121.5, 12.0, 180.0, 180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A"};
    }

    @Test
    @DisplayName("MySQL 全挂 spool 落盘，恢复后回放进主表并清文件")
    void spoolWhileDownReplayAfterRecovery() throws Exception {
        FileFallbackStore store = new FileFallbackStore(tempDir, 10L * 1024L * 1024L,
                100L * 1024L * 1024L);

        // 阶段一：DB 全挂（主表+兜底表都抛）→ saveGps 转磁盘 spool
        JdbcTemplate deadJt = mock(JdbcTemplate.class);
        when(deadJt.update(anyString(), any(Object[].class)))
                .thenThrow(new RuntimeException("MySQL down"));
        EdgeProperties properties = new EdgeProperties();
        properties.setMmsi("413999999");
        properties.setSchemaReady(true);
        properties.getCollect().getPersist().setEnabled(true);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SmartShipMetrics metrics = new SmartShipMetrics(registry);
        NmeaDataPersistenceService deadService = new NmeaDataPersistenceService(
                deadJt, properties, null, metrics, store);
        deadService.saveGps("RMC", "SERIAL", 31.2, 121.5, 12.0, 180.0,
                180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A", "413999999");

        List<FileFallbackStore.PendingFile> pending = store.pendingFiles();
        assertEquals(1, pending.size(), "全挂时必须落盘一行");
        assertEquals(1.0, registry.find("smartship_persistence_fallback_total")
                .tags("type", "gps").counter().count());

        // 阶段二：DB 恢复 → 回放进主表
        JdbcTemplate liveJt = realDb();
        FallbackReplayer replayer = new FallbackReplayer(liveJt, properties, store, metrics);
        replayer.replay();

        Long rows = liveJt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class);
        assertEquals(1L, rows, "回放必须把 spool 行写进主表");
        assertTrue(store.pendingFiles().isEmpty(), "回放成功必须清文件");
        assertEquals(1.0, registry.find("smartship_persistence_replay_total")
                .tags("result", "success").counter().count());
    }

    @Test
    @DisplayName("回放仍失败保留现场并计数，不丢行")
    void failedReplayKeepsScene() throws Exception {
        FileFallbackStore store = new FileFallbackStore(tempDir, 10L * 1024L * 1024L,
                100L * 1024L * 1024L);
        store.spool("gps", "413999999", gpsArgs());

        JdbcTemplate deadJt = mock(JdbcTemplate.class);
        when(deadJt.update(anyString(), any(Object[].class)))
                .thenThrow(new RuntimeException("still down"));
        EdgeProperties properties = new EdgeProperties();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FallbackReplayer replayer = new FallbackReplayer(deadJt, properties, store,
                new SmartShipMetrics(registry));
        replayer.replay();

        assertEquals(1, store.pendingFiles().size(), "失败必须保留现场");
        assertEquals(1, store.readAll(store.pendingFiles().get(0)).records().size());
        assertEquals(1.0, registry.find("smartship_persistence_replay_total")
                .tags("result", "failure").counter().count());
    }

    private static final String FAILED_WRITES_DDL = """
            CREATE TABLE zncb_failed_writes (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                stream VARCHAR(32), mmsi VARCHAR(32),
                payload TEXT, error VARCHAR(500),
                create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""";

    private JdbcTemplate dbWithFallbackTable() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:dbreplay_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL",
                "sa", "");
        JdbcTemplate jt = new JdbcTemplate(ds);
        jt.execute(GPS_DDL);
        jt.execute(FAILED_WRITES_DDL);
        return jt;
    }

    @Test
    @DisplayName("DB兜底行回放进主表并删行")
    void dbFallbackRowsReplayIntoMainTable() {
        JdbcTemplate jt = dbWithFallbackTable();
        Object[] args = {"S001", "413999999", "RMC", "SERIAL",
                java.time.LocalDateTime.of(2026, 9, 19, 10, 0, 0),
                31.2, 121.5, 12.0, 180.0, 180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A"};
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, payload, error) VALUES (?,?,?,?)",
                "gps", "413999999",
                com.smartship.edge.persist.FileFallbackStore.argsToJson("gps", "413999999", args),
                "boom");

        EdgeProperties properties = new EdgeProperties();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties, new FileFallbackStore(tempDir, 1024L, 4096L), new SmartShipMetrics(registry));
        replayer.replay();

        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class),
                "DB兜底行必须重写入主表");
        assertEquals(0L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "回放成功必须删行");
        assertEquals(1.0, registry.find("smartship_persistence_replay_total")
                .tags("result", "success").counter().count());
    }

    @Test
    @DisplayName("旧版诊断文本行解析失败留原地，不阻塞、不崩溃")
    void legacyNonJsonRowLeftInPlace() {
        JdbcTemplate jt = dbWithFallbackTable();
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, payload, error) VALUES (?,?,?,?)",
                "gps", "413999999", "{ship_id=S001, mmsi=413999999}", "legacy");

        EdgeProperties properties = new EdgeProperties();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties, new FileFallbackStore(tempDir, 1024L, 4096L), new SmartShipMetrics(registry));
        replayer.replay();

        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "毒行保留供人工审计");
        assertEquals(0L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class));
    }

    @Test
    @DisplayName("毒行超限后多轮回放不再重复解析：稳定保留、主表始终为空")
    void poisonRowStableAcrossCycles() {
        JdbcTemplate jt = dbWithFallbackTable();
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, payload, error) VALUES (?,?,?,?)",
                "gps", "413999999", "not-json-at-all", "legacy");

        EdgeProperties properties = new EdgeProperties();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties, new FileFallbackStore(tempDir, 1024L, 4096L), new SmartShipMetrics(registry));
        for (int i = 0; i < 2 * FallbackReplayer.MAX_ROW_ATTEMPTS + 2; i++) {
            replayer.replay();
        }

        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "超限毒行保留供人工审计");
        assertEquals(0L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class),
                "毒行永不污染主表");
    }

    @Test
    @DisplayName("P2：毒行占满首屏也不饿死后面的正常行（游标分页）")
    void poisonRowsNeverStarveLaterRows() {
        JdbcTemplate jt = dbWithFallbackTable();
        for (int i = 0; i < 3; i++) {
            jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, payload, error) VALUES (?,?,?,?)",
                    "gps", "413999999", "poison-" + i, "legacy");
        }

        EdgeProperties properties = new EdgeProperties();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties, new FileFallbackStore(tempDir, 1024L, 4096L), new SmartShipMetrics(registry));
        // 先把毒行记次刷满（模拟长期滞留）
        for (int i = 0; i < FallbackReplayer.MAX_ROW_ATTEMPTS + 1; i++) {
            replayer.replayDbTable(10);
        }

        // 正常行 id 更大、排在毒行后面，预算仅 2（首屏装不下正常行）
        Object[] args = {"S001", "413999999", "RMC", "SERIAL",
                java.time.LocalDateTime.of(2026, 9, 19, 10, 0, 0),
                31.2, 121.5, 12.0, 180.0, 180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A"};
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, payload, error) VALUES (?,?,?,?)",
                "gps", "413999999",
                com.smartship.edge.persist.FileFallbackStore.argsToJson("gps", "413999999", args),
                "boom");
        replayer.replayDbTable(2);

        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class),
                "游标分页必须越过超限毒行回放正常行");
        assertEquals(3L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "毒行保留供人工审计");
    }

    @Test
    @DisplayName("文件逐行落盘进度：两行成功触发两次回写")
    void fileProgressPersistedPerRow() throws Exception {
        JdbcTemplate jt = realDb();
        com.smartship.edge.persist.FileFallbackStore store =
                org.mockito.Mockito.mock(com.smartship.edge.persist.FileFallbackStore.class);
        Object[] args = {"S001", "413999999", "RMC", "SERIAL",
                java.time.LocalDateTime.of(2026, 9, 19, 10, 0, 0),
                31.2, 121.5, 12.0, 180.0, 180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A"};
        com.smartship.edge.persist.FileFallbackStore.ParsedLine parsed =
                com.smartship.edge.persist.FileFallbackStore.parseLine(
                        com.smartship.edge.persist.FileFallbackStore.argsToJson("gps", "413999999", args));
        com.smartship.edge.persist.FileFallbackStore.ReplayRecord rec =
                new com.smartship.edge.persist.FileFallbackStore.ReplayRecord(
                        "gps", "413999999", parsed.args(), "line-stub");
        com.smartship.edge.persist.FileFallbackStore.PendingFile file =
                new com.smartship.edge.persist.FileFallbackStore.PendingFile(
                        tempDir.resolve("stub.jsonl"));
        org.mockito.Mockito.when(store.readAll(file)).thenReturn(
                new com.smartship.edge.persist.FileFallbackStore.ReadResult(
                        java.util.List.of(rec, rec), 0));

        EdgeProperties properties = new EdgeProperties();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties, store,
                new SmartShipMetrics(new SimpleMeterRegistry()));
        replayer.replayFile(file, 10);

        assertEquals(2L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class));
        org.mockito.Mockito.verify(store, org.mockito.Mockito.times(2))
                .rewrite(org.mockito.Mockito.eq(file), org.mockito.Mockito.anyList());
    }
}
