package com.smartship.edge.benchmark.support;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Benchmark 共享 H2 夹具（测试作用域）。
 * <p>
 * 说明：生产 {@code ship-schema.sql} 含 MySQL 方言（ENGINE / COMMENT / ON UPDATE），
 * H2 无法逐字执行；此处按生产持久化 SQL 实际使用的列子集建最小等价表，
 * 覆盖 {@code saveGps / saveWind / saveDepth / saveRudder / saveEngine} 与上传游标。
 */
public final class BenchmarkFixtures {

    private BenchmarkFixtures() {
    }

    public static JdbcTemplate newH2(String dbName) {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "");
        return new JdbcTemplate(ds);
    }

    public static void createShipTables(JdbcTemplate jt) {
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_gps_data (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    ship_id VARCHAR(64), mmsi VARCHAR(32), sentence_type VARCHAR(16), replay_id VARCHAR(64) NULL,
                    source VARCHAR(32), timestamp DATETIME,
                    latitude DOUBLE, longitude DOUBLE, speed_knots DOUBLE,
                    course_over_ground DOUBLE, heading_true DOUBLE, heading_magnetic DOUBLE,
                    magnetic_variation DOUBLE, altitude_m DOUBLE, satellites INT,
                    hdop DOUBLE, position_quality INT, gps_status VARCHAR(16)
                )""");
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_wind_data (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    ship_id VARCHAR(64), mmsi VARCHAR(32), sentence_type VARCHAR(16), replay_id VARCHAR(64) NULL,
                    source VARCHAR(32), timestamp DATETIME,
                    apparent_wind_angle DOUBLE, apparent_wind_speed DOUBLE,
                    true_wind_angle DOUBLE, true_wind_direction DOUBLE, true_wind_speed DOUBLE
                )""");
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_depth_data (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    ship_id VARCHAR(64), mmsi VARCHAR(32), sentence_type VARCHAR(16), replay_id VARCHAR(64) NULL,
                    source VARCHAR(32), timestamp DATETIME,
                    depth_m DOUBLE, transducer_offset_m DOUBLE
                )""");
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_rudder_data (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    ship_id VARCHAR(64), mmsi VARCHAR(32), sentence_type VARCHAR(16), replay_id VARCHAR(64) NULL,
                    source VARCHAR(32), timestamp DATETIME, rudder_angle DOUBLE
                )""");
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_engine_data (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    ship_id VARCHAR(64), mmsi VARCHAR(32), slave_id INT,
                    protocol VARCHAR(20) DEFAULT 'MODBUS_TCP', timestamp DATETIME,
                    rpm DOUBLE, coolant_temp DOUBLE, lube_oil_press DOUBLE,
                    fuel_press DOUBLE, exhaust_temp DOUBLE, tc_air_press DOUBLE,
                    start_air_press DOUBLE, bearing_temp DOUBLE, battery_volt DOUBLE,
                    running_hours INT, status INT, alarm_bits1 INT, alarm_bits2 INT,
                    replay_id VARCHAR(64) NULL
                )""");
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_upload_cursor (
                    stream_name VARCHAR(64) NOT NULL,
                    partition_key VARCHAR(64) NOT NULL DEFAULT '',
                    last_uploaded_id BIGINT NOT NULL DEFAULT 0,
                    last_uploaded_time DATETIME NULL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stream_name, partition_key)
                )""");
    }

    public static long count(JdbcTemplate jt, String table) {
        Long v = jt.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
        return v == null ? 0L : v;
    }

    public static long maxId(JdbcTemplate jt, String table) {
        Long v = jt.queryForObject("SELECT COALESCE(MAX(id), 0) FROM " + table, Long.class);
        return v == null ? 0L : v;
    }
}
