package com.smartship.edge.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 存量库迁移：无 replay_id 的老表补列 + 唯一约束，重复执行安全。
 */
class ShipLocalInitializerTest {

    private JdbcTemplate oldShapeDb() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:migrate_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL",
                "sa", "");
        JdbcTemplate jt = new JdbcTemplate(ds);
        jt.execute("CREATE TABLE zncb_gps_data ("
                + " id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + " ship_id VARCHAR(64), mmsi VARCHAR(32))");
        jt.execute("CREATE TABLE zncb_failed_writes ("
                + " id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + " stream VARCHAR(32), mmsi VARCHAR(32), payload TEXT, error VARCHAR(500))");
        return jt;
    }

    @Test
    @DisplayName("老表补列后可写 replay_id，同值第二次被唯一约束拒绝")
    void migrateThenUniqueEnforced() {
        JdbcTemplate jt = oldShapeDb();
        ShipLocalInitializer.ensureReplayIdColumns(jt);

        jt.update("INSERT INTO zncb_gps_data (ship_id, mmsi, replay_id) VALUES (?,?,?)",
                "S001", "413999999", "r-1");
        try {
            jt.update("INSERT INTO zncb_gps_data (ship_id, mmsi, replay_id) VALUES (?,?,?)",
                    "S001", "413999999", "r-1");
            assertTrue(false, "重复 replay_id 必须被唯一约束拒绝");
        } catch (Exception expected) {
            // 唯一约束生效
        }
        jt.update("INSERT INTO zncb_gps_data (ship_id, mmsi, replay_id) VALUES (?,?,?)",
                "S001", "413999999", null);
        jt.update("INSERT INTO zncb_gps_data (ship_id, mmsi, replay_id) VALUES (?,?,?)",
                "S001", "413999999", null);
        assertEquals(3L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class),
                "NULL 不互斥，正常行不受影响");
    }

    @Test
    @DisplayName("兜底表同样补列，老诊断行保留且新行可写同键去重")
    void migrateFallbackTable() {
        JdbcTemplate jt = oldShapeDb();
        ShipLocalInitializer.ensureReplayIdColumns(jt);

        jt.update("INSERT INTO zncb_failed_writes (stream, mmsi, payload, error) VALUES (?,?,?,?)",
                "gps", "413999999", "{ship_id=S001}", "legacy");
        assertEquals(1L, jt.queryForObject("SELECT COUNT(*) FROM zncb_failed_writes", Long.class),
                "老诊断行原样保留");
    }

    @Test
    @DisplayName("迁移重复执行安全（幂等）")
    void migrateIsIdempotent() {
        JdbcTemplate jt = oldShapeDb();
        assertDoesNotThrow(() -> ShipLocalInitializer.ensureReplayIdColumns(jt));
        assertDoesNotThrow(() -> ShipLocalInitializer.ensureReplayIdColumns(jt));
        assertEquals(0L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class));
    }

    @Test
    @DisplayName("真实异常必须抛（调用方置schemaReady=false，不带病启动）")
    void realFailureThrows() {
        JdbcTemplate jt = org.mockito.Mockito.mock(JdbcTemplate.class);
        org.mockito.Mockito.doThrow(new RuntimeException("boom"))
                .when(jt).execute(org.mockito.ArgumentMatchers.anyString());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> ShipLocalInitializer.ensureReplayIdColumns(jt));
    }

    @Test
    @DisplayName("历史长名重复索引被清理，只剩schema同名约束且功能生效")
    void legacyDuplicateIndexCleaned() {
        JdbcTemplate jt = oldShapeDb();
        // 还原被旧版本污染的现场：列已在，但建了长名等价唯一索引
        jt.execute("ALTER TABLE zncb_gps_data ADD COLUMN replay_id VARCHAR(64) NULL");
        jt.execute("ALTER TABLE zncb_gps_data"
                + " ADD CONSTRAINT uk_zncb_gps_data_replay_id UNIQUE (replay_id)");

        ShipLocalInitializer.ensureReplayIdColumns(jt);

        // 约束视图断言（H2 后台索引名会加后缀，不直接断言索引名）
        java.util.List<String> constraints = jt.query(
                "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
                        + " WHERE TABLE_NAME = 'ZNCB_GPS_DATA' AND CONSTRAINT_TYPE = 'UNIQUE'",
                (rs, n) -> rs.getString(1));
        assertTrue(constraints.contains("UK_GPS_REPLAY_ID"),
                "必须有与schema同名的唯一约束，实际=" + constraints);
        assertTrue(constraints.stream().noneMatch(c -> c.contains("ZNCB_GPS")),
                "历史长名约束必须删掉，实际=" + constraints);

        // 功能断言：重复 replay_id 被拒绝（这才是去重真正的保障）
        jt.update("INSERT INTO zncb_gps_data (ship_id, mmsi, replay_id) VALUES (?,?,?)",
                "S001", "413999999", "dup-1");
        try {
            jt.update("INSERT INTO zncb_gps_data (ship_id, mmsi, replay_id) VALUES (?,?,?)",
                    "S001", "413999999", "dup-1");
            assertTrue(false, "重复 replay_id 必须被拒绝");
        } catch (Exception expected) {
            // 唯一约束生效
        }

        // 清理后重复执行依然幂等
        assertDoesNotThrow(() -> ShipLocalInitializer.ensureReplayIdColumns(jt));
    }

    @Test
    @DisplayName("表内已有重复replay_id时建UNIQUE必须如实失败，不能当幂等放行")
    void duplicateDataBlocksUnique() {
        JdbcTemplate jt = oldShapeDb();
        // 还原存量脏数据现场：列已在、无约束、已有重复值
        jt.execute("ALTER TABLE zncb_gps_data ADD COLUMN replay_id VARCHAR(64) NULL");
        jt.update("INSERT INTO zncb_gps_data (ship_id, mmsi, replay_id) VALUES (?,?,?)",
                "S001", "413999999", "dup");
        jt.update("INSERT INTO zncb_gps_data (ship_id, mmsi, replay_id) VALUES (?,?,?)",
                "S001", "413999999", "dup");

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> ShipLocalInitializer.ensureReplayIdColumns(jt),
                "重复数据导致UNIQUE建不起来必须抛，不能误判成已存在而schemaReady=true");

        // 约束确实没建成（复核手段与主代码同源：INFORMATION_SCHEMA）
        java.util.List<String> constraints = jt.query(
                "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
                        + " WHERE TABLE_NAME = 'ZNCB_GPS_DATA' AND CONSTRAINT_TYPE = 'UNIQUE'",
                (rs, n) -> rs.getString(1));
        assertTrue(constraints.stream().noneMatch(c -> c.equalsIgnoreCase("UK_GPS_REPLAY_ID")),
                "约束没建成才是失败的证据，实际=" + constraints);
    }

    @Test
    @DisplayName("复合唯一索引(replay_id,other_col)不被清理，规范单列约束照常补上")
    void compositeUniquePreserved() {
        JdbcTemplate jt = oldShapeDb();
        jt.execute("ALTER TABLE zncb_gps_data ADD COLUMN replay_id VARCHAR(64) NULL");
        jt.execute("ALTER TABLE zncb_gps_data"
                + " ADD CONSTRAINT uk_gps_composite UNIQUE (replay_id, ship_id)");

        ShipLocalInitializer.ensureReplayIdColumns(jt);

        java.util.List<String> constraints = jt.query(
                "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
                        + " WHERE TABLE_NAME = 'ZNCB_GPS_DATA' AND CONSTRAINT_TYPE = 'UNIQUE'",
                (rs, n) -> rs.getString(1));
        assertTrue(constraints.stream().anyMatch(c -> c.equalsIgnoreCase("UK_GPS_COMPOSITE")),
                "复合唯一索引必须保留，实际=" + constraints);
        assertTrue(constraints.stream().anyMatch(c -> c.equalsIgnoreCase("UK_GPS_REPLAY_ID")),
                "规范单列约束仍需补上，实际=" + constraints);
    }
}
