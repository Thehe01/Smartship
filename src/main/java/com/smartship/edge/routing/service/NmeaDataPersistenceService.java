package com.smartship.edge.routing.service;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PersistenceThrottle;
import com.smartship.edge.routing.ShipDataSourceManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * 边缘端时序数据持久化服务
 * <p>
 * 采用 @Async 异步入库与采集主循环解耦，通过 ShipDataSourceManager 动态路由至指定船库
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NmeaDataPersistenceService {

    private final ShipDataSourceManager shipDataSourceManager;
    private final EdgeProperties properties;
    private final PersistenceThrottle throttle;

    private JdbcTemplate getJdbcTemplate(String mmsi) {
        return shipDataSourceManager.getJdbcTemplate(null, mmsi);
    }

    private String currentShipId() {
        String sid = properties.getShipId();
        String mmsi = properties.getMmsi();
        return (sid != null && !sid.isEmpty()) ? sid : mmsi;
    }

    @Async("persistenceExecutor")
    public void saveGps(String sentenceType, String source,
                        Double lat, Double lon, Double speedKnots, Double course,
                        Double headingTrue, Double headingMag, Double magVar,
                        Double altitude, Integer satellites, Double hdop,
                        Integer quality, String gpsStatus, String mmsi) {
        if (!properties.getCollect().getPersist().isEnabled()) return;
        if (mmsi == null || !properties.isSchemaReady()) return;
        try {
            String shipId = currentShipId();
            getJdbcTemplate(mmsi).update("""
                    INSERT INTO zncb_gps_data (ship_id, mmsi, sentence_type, source, timestamp,
                        latitude, longitude, speed_knots, course_over_ground,
                        heading_true, heading_magnetic, magnetic_variation,
                        altitude_m, satellites, hdop, position_quality, gps_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    shipId, mmsi, sentenceType, source, LocalDateTime.now(),
                    lat, lon, speedKnots, course,
                    headingTrue, headingMag, magVar,
                    altitude, satellites, hdop, quality, gpsStatus);
        } catch (Exception e) {
            log.warn("[Persist-GPS] 写入异常: {}", e.getMessage());
        }
    }

    @Async("persistenceExecutor")
    public void saveWind(String sentenceType, String source,
                         Double apparentAngle, Double apparentSpeed,
                         Double trueAngle, Double trueDirection, Double trueSpeed, String mmsi) {
        if (!properties.getCollect().getPersist().isEnabled()) return;
        if (mmsi == null || !properties.isSchemaReady()) return;
        try {
            String shipId = currentShipId();
            getJdbcTemplate(mmsi).update("""
                    INSERT INTO zncb_wind_data (ship_id, mmsi, sentence_type, source, timestamp,
                        apparent_wind_angle, apparent_wind_speed,
                        true_wind_angle, true_wind_direction, true_wind_speed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    shipId, mmsi, sentenceType, source, LocalDateTime.now(),
                    apparentAngle, apparentSpeed,
                    trueAngle, trueDirection, trueSpeed);
        } catch (Exception e) {
            log.warn("[Persist-Wind] 写入异常: {}", e.getMessage());
        }
    }

    @Async("persistenceExecutor")
    public void saveDepth(String sentenceType, String source, Double depthM, Double offsetM, String mmsi) {
        if (!properties.getCollect().getPersist().isEnabled()) return;
        if (mmsi == null || !properties.isSchemaReady()) return;
        try {
            String shipId = currentShipId();
            getJdbcTemplate(mmsi).update("""
                    INSERT INTO zncb_depth_data (ship_id, mmsi, sentence_type, source, timestamp,
                        depth_m, transducer_offset_m)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    shipId, mmsi, sentenceType, source, LocalDateTime.now(), depthM, offsetM);
        } catch (Exception e) {
            log.warn("[Persist-Depth] 写入异常: {}", e.getMessage());
        }
    }

    @Async("persistenceExecutor")
    public void saveRudder(String sentenceType, String source, Double rudderAngle, String mmsi) {
        if (!properties.getCollect().getPersist().isEnabled()) return;
        if (mmsi == null || !properties.isSchemaReady()) return;
        try {
            String shipId = currentShipId();
            getJdbcTemplate(mmsi).update("""
                    INSERT INTO zncb_rudder_data (ship_id, mmsi, sentence_type, source, timestamp, rudder_angle)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    shipId, mmsi, sentenceType, source, LocalDateTime.now(), rudderAngle);
        } catch (Exception e) {
            log.warn("[Persist-Rudder] 写入异常: {}", e.getMessage());
        }
    }

    @Async("persistenceExecutor")
    public void saveEngine(String mmsi, int slaveId, double rpm, double coolantTemp,
                           double lubeOilPress, double fuelPress, double exhaustTemp,
                           double tcAirPress, double startAirPress, double bearingTemp,
                           double batteryVolt, int runningHours, int status,
                           int alarmBits1, int alarmBits2) {
        if (!properties.getCollect().getPersist().isEnabled()) return;
        if (mmsi == null || !properties.isSchemaReady()) return;
        try {
            String shipId = currentShipId();
            getJdbcTemplate(mmsi).update("""
                    INSERT INTO zncb_engine_data (ship_id, mmsi, slave_id, protocol, timestamp,
                        rpm, coolant_temp, lube_oil_press, fuel_press, exhaust_temp,
                        tc_air_press, start_air_press, bearing_temp, battery_volt,
                        running_hours, status, alarm_bits1, alarm_bits2)
                    VALUES (?, ?, ?, 'MODBUS_TCP', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    shipId, mmsi, slaveId, LocalDateTime.now(),
                    rpm, coolantTemp, lubeOilPress, fuelPress, exhaustTemp,
                    tcAirPress, startAirPress, bearingTemp, batteryVolt,
                    runningHours, status, alarmBits1, alarmBits2);
        } catch (Exception e) {
            log.warn("[Persist-Engine] 写入异常: {}", e.getMessage());
        }
    }
}
