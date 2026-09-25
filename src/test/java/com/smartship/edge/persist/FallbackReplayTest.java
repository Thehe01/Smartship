package com.smartship.edge.persist;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
                hdop DOUBLE, position_quality INT, gps_status VARCHAR(16),
                replay_id VARCHAR(64) NULL,
                CONSTRAINT uk_gps_replay_id UNIQUE (replay_id)
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
        store.spool("gps", "413999999", "r-down-1", gpsArgs());

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
                replay_id VARCHAR(64) NULL,
                payload TEXT, error VARCHAR(500),
                create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uk_failed_replay_id UNIQUE (replay_id)
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
                com.smartship.edge.persist.FileFallbackStore.argsToJson("gps", "413999999", "r-json-1", args),
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
                com.smartship.edge.persist.FileFallbackStore.argsToJson("gps", "413999999", "r-json-1", args),
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
                        com.smartship.edge.persist.FileFallbackStore.argsToJson("gps", "413999999", "r-json-1", args));
        com.smartship.edge.persist.FileFallbackStore.ParsedLine parsed2 =
                com.smartship.edge.persist.FileFallbackStore.parseLine(
                        com.smartship.edge.persist.FileFallbackStore.argsToJson("gps", "413999999", "r-json-2", args));
        com.smartship.edge.persist.FileFallbackStore.ReplayRecord rec =
                new com.smartship.edge.persist.FileFallbackStore.ReplayRecord(
                        "gps", "413999999", parsed.replayId(), parsed.args(), "line-stub");
        com.smartship.edge.persist.FileFallbackStore.ReplayRecord rec2 =
                new com.smartship.edge.persist.FileFallbackStore.ReplayRecord(
                        "gps", "413999999", parsed2.replayId(), parsed2.args(), "line-stub-2");
        com.smartship.edge.persist.FileFallbackStore.PendingFile file =
                new com.smartship.edge.persist.FileFallbackStore.PendingFile(
                        tempDir.resolve("stub.jsonl"));
        org.mockito.Mockito.when(store.readAll(file)).thenReturn(
                new com.smartship.edge.persist.FileFallbackStore.ReadResult(
                        java.util.List.of(rec, rec2), 0));

        EdgeProperties properties = new EdgeProperties();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties, store,
                new SmartShipMetrics(new SimpleMeterRegistry()));
        replayer.replayFile(file, 10);

        assertEquals(2L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class));
        org.mockito.Mockito.verify(store, org.mockito.Mockito.times(2))
                .rewrite(org.mockito.Mockito.eq(file), org.mockito.Mockito.anyList());
    }

    @Test
    @DisplayName("崩溃模拟：主表已写、兜底行未删，再次回放仍只有一条")
    void crashBetweenInsertAndDeleteStaysSingleRow() {
        JdbcTemplate jt = dbWithFallbackTable();
        Object[] args = {"S001", "413999999", "RMC", "SERIAL",
                java.time.LocalDateTime.of(2026, 9, 19, 10, 0, 0),
                31.2, 121.5, 12.0, 180.0, 180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A"};
        String payload = com.smartship.edge.persist.FileFallbackStore.argsToJson(
                "gps", "413999999", "crash-sim-replay-id-1", args);
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, replay_id, payload, error)"
                        + " VALUES (?,?,?,?,?)",
                "gps", "413999999", "crash-sim-replay-id-1", payload, "boom");

        EdgeProperties properties = new EdgeProperties();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties, new FileFallbackStore(tempDir, 1024L, 4096L), new SmartShipMetrics(registry));
        // 第一轮正常回放：主表 1 行，兜底删行
        replayer.replay();
        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class));

        // 还原崩溃现场：主表行已在，兜底行“没删掉”（同一 replay_id 再次出现）
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, replay_id, payload, error)"
                        + " VALUES (?,?,?,?,?)",
                "gps", "413999999", "crash-sim-replay-id-1", payload, "boom");
        replayer.replay();

        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class),
                "唯一约束必须吸收重复回放，主表仍只有一条");
        assertEquals(0L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "吸收后兜底行照常删除");
    }

    @Test
    @DisplayName("P2：前面5000毒行，后面正常行同轮仍被扫描到")
    void fiveThousandPoisonRowsDoNotStarveGoodRow() {
        JdbcTemplate jt = dbWithFallbackTable();
        StringBuilder sb = new StringBuilder(
                "INSERT INTO zncb_failed_writes (stream, mmsi, payload, error) VALUES ");
        for (int i = 0; i < 5000; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("('gps','413999999','poison-").append(i).append("','legacy')");
        }
        jt.execute(sb.toString());

        Object[] args = {"S001", "413999999", "RMC", "SERIAL",
                java.time.LocalDateTime.of(2026, 9, 19, 10, 0, 0),
                31.2, 121.5, 12.0, 180.0, 180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A"};
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, payload, error) VALUES (?,?,?,?)",
                "gps", "413999999",
                com.smartship.edge.persist.FileFallbackStore.argsToJson("gps", "413999999", "r-json-1", args),
                "boom");

        EdgeProperties properties = new EdgeProperties();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties, new FileFallbackStore(tempDir, 1024L, 4096L),
                new SmartShipMetrics(new SimpleMeterRegistry()));
        // 单轮、默认预算：毒行不消耗 budget，游标翻页直达正常行
        replayer.replayDbTable(200);

        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class),
                "5000毒行之后正常行必须同轮回放");
        assertEquals(5000L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "毒行保留供人工审计");
    }

    @Test
    @DisplayName("P1：失败分级——确定性数据错误才记毒，未知一律按瞬时重试")
    void failureClassification() {
        // 确定性：积极信号才记毒
        assertTrue(FallbackReplayer.isDeterministicFailure(
                new org.springframework.dao.DataIntegrityViolationException("too long")));
        assertTrue(FallbackReplayer.isDeterministicFailure(
                new org.springframework.jdbc.BadSqlGrammarException(
                        "t", "SELECT x", new java.sql.SQLException("table not found", "42S02"))));
        assertTrue(FallbackReplayer.isDeterministicFailure(
                new java.sql.SQLException("value too long", "22001")));
        assertTrue(FallbackReplayer.isDeterministicFailure(
                new java.sql.SQLException("null violation", "23502")));
        assertTrue(FallbackReplayer.isDeterministicFailure(
                new RuntimeException("wrap",
                        new org.springframework.dao.DataIntegrityViolationException("c"))));

        // 瞬时：未知一律按瞬时（宁可多重试，绝不错杀）
        assertFalse(FallbackReplayer.isDeterministicFailure(new RuntimeException("boom")));
        assertFalse(FallbackReplayer.isDeterministicFailure(
                new java.sql.SQLException("Communications link failure", "08S01")));
        assertFalse(FallbackReplayer.isDeterministicFailure(
                new java.sql.SQLTransientConnectionException("Connection is not available")));
        assertFalse(FallbackReplayer.isDeterministicFailure(
                new RuntimeException(new java.sql.SQLTimeoutException(
                        "Timeout trying to lock table", "HYT01", 40001))));
        // 重复键已在上游吸收为成功，永不记毒
        assertFalse(FallbackReplayer.isDeterministicFailure(
                new org.springframework.dao.DuplicateKeyException("dup")));
    }

    @Test
    @DisplayName("P1：确定性坏行记毒跳过，但同轮不堵住后面正常行")
    void deterministicBadRowDoesNotStallQueue() {
        JdbcTemplate jt = dbWithFallbackTable();
        Object[] badArgs = gpsArgs();
        badArgs[0] = "S".repeat(100); // ship_id VARCHAR(64)：超长必败，且每次必败
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, replay_id, payload, error)"
                        + " VALUES (?,?,?,?,?)",
                "gps", "413999999", "bad-key-1",
                com.smartship.edge.persist.FileFallbackStore.argsToJson(
                        "gps", "413999999", "bad-key-1", badArgs),
                "boom");
        Object[] goodArgs = gpsArgs();
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, replay_id, payload, error)"
                        + " VALUES (?,?,?,?,?)",
                "gps", "413999999", "good-key-1",
                com.smartship.edge.persist.FileFallbackStore.argsToJson(
                        "gps", "413999999", "good-key-1", goodArgs),
                "boom");

        EdgeProperties properties = new EdgeProperties();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties,
                new FileFallbackStore(tempDir, 1024L, 4096L),
                new SmartShipMetrics(new SimpleMeterRegistry()));

        // 首轮：坏行记毒但不截断，正常行同轮回放（旧代码这里会 dbDown 截断，好行熬到第 6 轮才见天日）
        replayer.replayDbTable(10);
        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class),
                "确定性坏行不能堵住后面正常行");
        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "坏行保留供人工审计");

        // 多轮后依然稳定：坏行超限跳过，好行不受影响
        for (int i = 0; i < FallbackReplayer.MAX_ROW_ATTEMPTS + 1; i++) {
            replayer.replayDbTable(10);
        }
        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class));
        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "超限坏行保留供人工审计");
    }

    @Test
    @DisplayName("P1：瞬时故障（锁等待超时）连败6轮不记毒，恢复后照常回放")
    void transientFailuresNeverPoison() throws Exception {
        JdbcTemplate jt = dbWithFallbackTable();
        Object[] args = gpsArgs();
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, replay_id, payload, error)"
                        + " VALUES (?,?,?,?,?)",
                "gps", "413999999", "transient-key-1",
                com.smartship.edge.persist.FileFallbackStore.argsToJson(
                        "gps", "413999999", "transient-key-1", args),
                "boom");

        EdgeProperties properties = new EdgeProperties();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties,
                new FileFallbackStore(tempDir, 1024L, 4096L),
                new SmartShipMetrics(new SimpleMeterRegistry()));

        // 占锁：另一连接预插入同 replay_id 未提交 → 回放 INSERT 被锁住直到超时（真瞬时故障）
        java.sql.Connection locker = jt.getDataSource().getConnection();
        locker.setAutoCommit(false);
        try {
            try (java.sql.PreparedStatement ps = locker.prepareStatement(
                    "INSERT INTO zncb_gps_data (ship_id, mmsi, sentence_type, source,"
                            + " timestamp, latitude, longitude, speed_knots, course_over_ground,"
                            + " heading_true, heading_magnetic, magnetic_variation, altitude_m,"
                            + " satellites, hdop, position_quality, gps_status, replay_id)"
                            + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                Object[] full = java.util.Arrays.copyOf(args, args.length + 1);
                full[args.length] = "transient-key-1";
                for (int i = 0; i < full.length; i++) {
                    ps.setObject(i + 1, full[i]);
                }
                ps.executeUpdate();
            }
            // 6 轮（> MAX_ROW_ATTEMPTS）：旧代码会把合法行熬成毒行永久跳过
            for (int i = 0; i < FallbackReplayer.MAX_ROW_ATTEMPTS + 1; i++) {
                replayer.replayDbTable(10);
            }
            assertEquals(1L,
                    jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                    "瞬时故障再多轮也不能把合法行熬成毒行");
            locker.rollback();
        } finally {
            locker.close();
        }

        // 锁释放（DB 恢复）后照常回放：旧代码下这行已被永久跳过，这里会失败
        replayer.replayDbTable(10);
        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class),
                "恢复后合法行必须照常回放");
        assertEquals(0L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "回放成功必须删行");
    }

    @Test
    @DisplayName("规格c：同键同时存在DB兜底+spool，最终主表仍1条")
    void sameKeyInDbAndSpoolStaysSingleRow() throws Exception {
        JdbcTemplate jt = dbWithFallbackTable();
        Object[] args = {"S001", "413999999", "RMC", "SERIAL",
                java.time.LocalDateTime.of(2026, 9, 19, 10, 0, 0),
                31.2, 121.5, 12.0, 180.0, 180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A"};
        String sharedKey = "shared-key-c-1";
        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, replay_id, payload, error)"
                        + " VALUES (?,?,?,?,?)",
                "gps", "413999999", sharedKey,
                com.smartship.edge.persist.FileFallbackStore.argsToJson(
                        "gps", "413999999", sharedKey, args),
                "boom");

        com.smartship.edge.persist.FileFallbackStore store =
                new com.smartship.edge.persist.FileFallbackStore(tempDir, 1024L * 1024L, 10L * 1024L * 1024L);
        store.spool("gps", "413999999", sharedKey, args);

        EdgeProperties properties = new EdgeProperties();
        FallbackReplayer replayer = new FallbackReplayer(jt, properties, store,
                new SmartShipMetrics(new SimpleMeterRegistry()));
        replayer.replay();

        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class),
                "同键双通道回放必须去重，主表仍1条");
        assertEquals(0L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "DB兜底行照常删除");
        assertTrue(store.pendingFiles().isEmpty(), "spool文件照常清理");
    }
}
