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
    @DisplayName("迁移重复执行安全（幂等）")
    void migrateIsIdempotent() {
        JdbcTemplate jt = oldShapeDb();
        assertDoesNotThrow(() -> ShipLocalInitializer.ensureReplayIdColumns(jt));
        assertDoesNotThrow(() -> ShipLocalInitializer.ensureReplayIdColumns(jt));
        assertEquals(0L, jt.queryForObject("SELECT COUNT(*) FROM zncb_gps_data", Long.class));
    }
}
