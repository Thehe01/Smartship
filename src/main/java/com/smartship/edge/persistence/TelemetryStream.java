package com.smartship.edge.persistence;

/** Fixed telemetry schema allowlist. SQL identifiers never come from user input. */
public enum TelemetryStream {
    GPS("gps", "zncb_gps_data", """
INSERT INTO zncb_gps_data (ship_id, mmsi, sentence_type, source, timestamp,
                latitude, longitude, speed_knots, course_over_ground,
                heading_true, heading_magnetic, magnetic_variation,
                altitude_m, satellites, hdop, position_quality, gps_status, replay_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """),
    WIND("wind", "zncb_wind_data", """
INSERT INTO zncb_wind_data (ship_id, mmsi, sentence_type, source, timestamp,
                apparent_wind_angle, apparent_wind_speed,
                true_wind_angle, true_wind_direction, true_wind_speed, replay_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """),
    DEPTH("depth", "zncb_depth_data", """
INSERT INTO zncb_depth_data (ship_id, mmsi, sentence_type, source, timestamp,
                depth_m, transducer_offset_m, replay_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """),
    RUDDER("rudder", "zncb_rudder_data", """
INSERT INTO zncb_rudder_data (ship_id, mmsi, sentence_type, source, timestamp, rudder_angle, replay_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """),
    ENGINE("engine", "zncb_engine_data", """
INSERT INTO zncb_engine_data (ship_id, mmsi, slave_id, protocol, timestamp,
                rpm, coolant_temp, lube_oil_press, fuel_press, exhaust_temp,
                tc_air_press, start_air_press, bearing_temp, battery_volt,
                running_hours, status, alarm_bits1, alarm_bits2, replay_id)
            VALUES (?, ?, ?, 'MODBUS_TCP', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """);

    private final String key;
    private final String table;
    private final String insertSql;

    TelemetryStream(String key, String table, String insertSql) {
        this.key = key;
        this.table = table;
        this.insertSql = insertSql;
    }

    public String insertSql() { return insertSql; }
    public int argumentCount() { return (int) insertSql.chars().filter(c -> c == '?').count(); }

    public static TelemetryStream fromKey(String key) {
        if ("engine-batch".equals(key)) return ENGINE;
        for (TelemetryStream stream : values()) {
            if (stream.key.equals(key)) return stream;
        }
        throw new IllegalArgumentException("Unknown telemetry stream: " + key);
    }

    public static String requireTable(String table) {
        for (TelemetryStream stream : values()) {
            if (stream.table.equals(table)) return table;
        }
        throw new IllegalArgumentException("Unknown telemetry table: " + table);
    }
}
