package com.smartship.edge.routing.service;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.observability.SmartShipMetrics;
import com.smartship.edge.persist.FileFallbackStore;
import com.smartship.edge.routing.PersistenceThrottle;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * 边缘端时序数据持久化服务（单船单库，无分船路由）。
 *
 * <p>船端只存本船数据：直接使用 Spring 默认单数据源 {@link JdbcTemplate}（本地本船库）。
 * {@code mmsi} 参数仅作为行内业务字段写入，不再做数据源路由键。
 */
@Slf4j
@Service
public class NmeaDataPersistenceService {

    private final JdbcTemplate jdbcTemplate;
    private final EdgeProperties properties;
    private final PersistenceThrottle throttle;
    private final SmartShipMetrics metrics;
    private final FileFallbackStore fallbackStore;

    @Autowired
    public NmeaDataPersistenceService(JdbcTemplate jdbcTemplate,
                                      EdgeProperties properties,
                                      PersistenceThrottle throttle,
                                      SmartShipMetrics metrics,
                                      FileFallbackStore fallbackStore) {
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
        this.throttle = throttle;
        this.metrics = metrics;
        this.fallbackStore = fallbackStore;
    }

    public NmeaDataPersistenceService(JdbcTemplate jdbcTemplate,
                                      EdgeProperties properties,
                                      PersistenceThrottle throttle,
                                      SmartShipMetrics metrics) {
        this(jdbcTemplate, properties, throttle, metrics, new FileFallbackStore(properties));
    }

    public NmeaDataPersistenceService(JdbcTemplate jdbcTemplate,
                                      EdgeProperties properties,
                                      PersistenceThrottle throttle) {
        this(jdbcTemplate, properties, throttle, null);
    }

    private String currentShipId() {
        String sid = properties.getShipId();
        String mmsi = properties.getMmsi();
        return (sid != null && !sid.isEmpty()) ? sid : mmsi;
    }

    private static final String GPS_SQL = """
            INSERT INTO zncb_gps_data (ship_id, mmsi, sentence_type, source, timestamp,
                latitude, longitude, speed_knots, course_over_ground,
                heading_true, heading_magnetic, magnetic_variation,
                altitude_m, satellites, hdop, position_quality, gps_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String WIND_SQL = """
            INSERT INTO zncb_wind_data (ship_id, mmsi, sentence_type, source, timestamp,
                apparent_wind_angle, apparent_wind_speed,
                true_wind_angle, true_wind_direction, true_wind_speed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String DEPTH_SQL = """
            INSERT INTO zncb_depth_data (ship_id, mmsi, sentence_type, source, timestamp,
                depth_m, transducer_offset_m)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String RUDDER_SQL = """
            INSERT INTO zncb_rudder_data (ship_id, mmsi, sentence_type, source, timestamp, rudder_angle)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private static final String ENGINE_SQL = """
            INSERT INTO zncb_engine_data (ship_id, mmsi, slave_id, protocol, timestamp,
                rpm, coolant_temp, lube_oil_press, fuel_press, exhaust_temp,
                tc_air_press, start_air_press, bearing_temp, battery_volt,
                running_hours, status, alarm_bits1, alarm_bits2)
            VALUES (?, ?, ?, 'MODBUS_TCP', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String FALLBACK_SQL = """
            INSERT INTO zncb_failed_writes (stream, mmsi, payload, error)
            VALUES (?, ?, ?, ?)
            """;

    /**
     * 耐久写入：主表一次 + 瞬态重试一次，重试耗尽后转存本地兜底表。
     *
     * <p>调用方（采集聚合缓存）取走快照即清空内存，DB 失败若只记日志就会在进入本地
     * 可靠存储前丢数。本方法保证：瞬态抖动由一次即时重试吸收；持续故障转存
     * {@code zncb_failed_writes}（同库事务外 best-effort，附带流类型与截断参数），
     * 并累加 {@code smartship_persistence_fallback_total} 告警。返回 true 仅当主表
     * 落库成功；兜底表自身失败不抛异常（调用方仍按失败计数，绝不静默按成功算）。
     */
    private boolean updateDurable(String stream, String mmsi, String sql, Object[] args) {
        try {
            jdbcTemplate.update(sql, args);
            return true;
        } catch (Exception first) {
            try {
                jdbcTemplate.update(sql, args);
                return true;
            } catch (Exception second) {
                writeFallback(stream, mmsi, args, second.getMessage());
                log.warn("[Persist-{}] 主表写入两次均失败，已转本地兜底: {}",
                        stream, second.getMessage());
                return false;
            }
        }
    }

    private void writeFallback(String stream, String mmsi, Object[] args, String error) {
        // tier 1：同库兜底表（可查询、可审计；payload 存可回放 JSON，回放器会重写入主表）
        try {
            String payload = truncate(FileFallbackStore.argsToJson(stream, mmsi, args), 65535);
            String err = truncate(error, 500);
            jdbcTemplate.update(FALLBACK_SQL, stream, mmsi, payload, err);
            if (metrics != null) {
                metrics.recordPersistenceFallback(stream);
            }
            return;
        } catch (Exception e) {
            log.warn("[Persist-{}] 兜底表亦不可写（MySQL 可能整体故障），转磁盘 spool: {}",
                    stream, e.getMessage());
        }
        // tier 2：磁盘 spool（MySQL 全挂也丢不了，回放器稍后重放）
        try {
            long dropped = fallbackStore.spool(stream, mmsi, args);
            if (metrics != null) {
                metrics.recordPersistenceFallback(stream);
                if (dropped > 0) {
                    metrics.recordFallbackDropped(dropped);
                }
            }
        } catch (Exception e) {
            if (metrics != null) {
                metrics.recordFallbackDropped(1);
            }
            log.warn("[Persist-{}] 磁盘 spool 亦失败（本轮数据丢失，靠指标告警）: {}",
                    stream, e.getMessage());
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max);
    }

    /**
     * 流标识 → 主表 INSERT SQL（供磁盘 spool 回放器使用）。未知流返回 null。
     */
    public static String sqlForStream(String stream) {
        if (stream == null) {
            return null;
        }
        return switch (stream) {
            case "gps" -> GPS_SQL;
            case "wind" -> WIND_SQL;
            case "depth" -> DEPTH_SQL;
            case "rudder" -> RUDDER_SQL;
            case "engine", "engine-batch" -> ENGINE_SQL;
            default -> null;
        };
    }

    @Async("persistenceExecutor")
    public void saveGps(String sentenceType, String source,
                        Double lat, Double lon, Double speedKnots, Double course,
                        Double headingTrue, Double headingMag, Double magVar,
                        Double altitude, Integer satellites, Double hdop,
                        Integer quality, String gpsStatus, String mmsi) {
        if (!properties.getCollect().getPersist().isEnabled()) return;
        if (mmsi == null || !properties.isSchemaReady()) return;
        long startNanos = System.nanoTime();
        String shipId = currentShipId();
        Object[] args = new Object[]{shipId, mmsi, sentenceType, source, LocalDateTime.now(),
                lat, lon, speedKnots, course,
                headingTrue, headingMag, magVar,
                altitude, satellites, hdop, quality, gpsStatus};
        if (updateDurable("gps", mmsi, GPS_SQL, args)) {
            if (metrics != null) {
                metrics.recordPersistenceSuccess("gps", System.nanoTime() - startNanos);
            }
        } else {
            if (metrics != null) {
                metrics.recordPersistenceFailure("gps", System.nanoTime() - startNanos);
            }
            log.warn("[Persist-GPS] 写入失败已转兜底: mmsi={}", mmsi);
        }
    }

    @Async("persistenceExecutor")
    public void saveWind(String sentenceType, String source,
                         Double apparentAngle, Double apparentSpeed,
                         Double trueAngle, Double trueDirection, Double trueSpeed, String mmsi) {
        if (!properties.getCollect().getPersist().isEnabled()) return;
        if (mmsi == null || !properties.isSchemaReady()) return;
        long startNanos = System.nanoTime();
        String shipId = currentShipId();
        Object[] args = new Object[]{shipId, mmsi, sentenceType, source, LocalDateTime.now(),
                apparentAngle, apparentSpeed,
                trueAngle, trueDirection, trueSpeed};
        if (updateDurable("wind", mmsi, WIND_SQL, args)) {
            if (metrics != null) {
                metrics.recordPersistenceSuccess("wind", System.nanoTime() - startNanos);
            }
        } else {
            if (metrics != null) {
                metrics.recordPersistenceFailure("wind", System.nanoTime() - startNanos);
            }
            log.warn("[Persist-Wind] 写入失败已转兜底: mmsi={}", mmsi);
        }
    }

    @Async("persistenceExecutor")
    public void saveDepth(String sentenceType, String source, Double depthM, Double offsetM, String mmsi) {
        if (!properties.getCollect().getPersist().isEnabled()) return;
        if (mmsi == null || !properties.isSchemaReady()) return;
        long startNanos = System.nanoTime();
        String shipId = currentShipId();
        Object[] args = new Object[]{shipId, mmsi, sentenceType, source, LocalDateTime.now(), depthM, offsetM};
        if (updateDurable("depth", mmsi, DEPTH_SQL, args)) {
            if (metrics != null) {
                metrics.recordPersistenceSuccess("depth", System.nanoTime() - startNanos);
            }
        } else {
            if (metrics != null) {
                metrics.recordPersistenceFailure("depth", System.nanoTime() - startNanos);
            }
            log.warn("[Persist-Depth] 写入失败已转兜底: mmsi={}", mmsi);
        }
    }

    @Async("persistenceExecutor")
    public void saveRudder(String sentenceType, String source, Double rudderAngle, String mmsi) {
        if (!properties.getCollect().getPersist().isEnabled()) return;
        if (mmsi == null || !properties.isSchemaReady()) return;
        long startNanos = System.nanoTime();
        String shipId = currentShipId();
        Object[] args = new Object[]{shipId, mmsi, sentenceType, source, LocalDateTime.now(), rudderAngle};
        if (updateDurable("rudder", mmsi, RUDDER_SQL, args)) {
            if (metrics != null) {
                metrics.recordPersistenceSuccess("rudder", System.nanoTime() - startNanos);
            }
        } else {
            if (metrics != null) {
                metrics.recordPersistenceFailure("rudder", System.nanoTime() - startNanos);
            }
            log.warn("[Persist-Rudder] 写入失败已转兜底: mmsi={}", mmsi);
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
        long startNanos = System.nanoTime();
        String shipId = currentShipId();
        Object[] args = new Object[]{shipId, mmsi, slaveId, LocalDateTime.now(),
                rpm, coolantTemp, lubeOilPress, fuelPress, exhaustTemp,
                tcAirPress, startAirPress, bearingTemp, batteryVolt,
                runningHours, status, alarmBits1, alarmBits2};
        if (updateDurable("engine", mmsi, ENGINE_SQL, args)) {
            if (metrics != null) {
                metrics.recordPersistenceSuccess("engine", System.nanoTime() - startNanos);
            }
        } else {
            if (metrics != null) {
                metrics.recordPersistenceFailure("engine", System.nanoTime() - startNanos);
            }
            log.warn("[Persist-Engine] 写入失败已转兜底: mmsi={}", mmsi);
        }
    }

    /**
     * 主机工况批量写入（同步，单库一次事务）。
     *
     * <p>单船模式下不再按船分组：整批一次 {@code batch} 提交，未全成功即回滚后逐行补写。
     *
     * @return 实际写入行数
     */
    public int saveEngineBatch(List<EnginePoint> batch) {
        if (batch == null || batch.isEmpty()) {
            return 0;
        }
        if (!properties.getCollect().getPersist().isEnabled() || !properties.isSchemaReady()) {
            return 0;
        }
        String shipId = currentShipId();
        return writeEngineGroup(shipId, batch);
    }

    private static final String ENGINE_BATCH_SQL = ENGINE_SQL;

    private static Object[] engineArgs(String shipId, EnginePoint p) {
        return new Object[]{shipId, p.mmsi(), p.slaveId(), java.sql.Timestamp.valueOf(p.timestamp()),
                p.rpm(), p.coolantTemp(), p.lubeOilPress(), p.fuelPress(),
                p.exhaustTemp(), p.tcAirPress(), p.startAirPress(), p.bearingTemp(),
                p.batteryVolt(), p.runningHours(), p.status(),
                p.alarmBits1(), p.alarmBits2()};
    }

    /** 单库分组写入：显式事务整批提交，异常回滚后逐行补写，返回精确落库行数。 */
    private int writeEngineGroup(String shipId, List<EnginePoint> group) {
        List<Object[]> args = new ArrayList<>(group.size());
        for (EnginePoint p : group) {
            args.add(engineArgs(shipId, p));
        }
        javax.sql.DataSource ds = java.util.Objects.requireNonNull(
                jdbcTemplate.getDataSource(), "JdbcTemplate DataSource");
        long startNanos = System.nanoTime();
        try (java.sql.Connection con = ds.getConnection()) {
            con.setAutoCommit(false);
            try (java.sql.PreparedStatement ps = con.prepareStatement(ENGINE_BATCH_SQL)) {
                for (Object[] a : args) {
                    for (int i = 0; i < a.length; i++) {
                        ps.setObject(i + 1, a[i]);
                    }
                    ps.addBatch();
                }
                int rows = countBatchSuccess(ps.executeBatch(), args.size());
                if (rows == args.size()) {
                    con.commit();
                    if (metrics != null) {
                        metrics.recordPersistenceSuccess("engine-batch", System.nanoTime() - startNanos);
                    }
                    return rows;
                }
                con.rollback();
            } catch (Exception batchEx) {
                rollbackQuietly(con);
                if (metrics != null) {
                    metrics.recordPersistenceFailure("engine-batch", System.nanoTime() - startNanos);
                }
                log.debug("[Persist-EngineBatch] 整批回滚转逐行补写 ({} 行): {}",
                        args.size(), batchEx.getMessage());
            }
            int rows = writeEngineRowsFallback(args);
            if (rows != args.size()) {
                log.warn("[Persist-EngineBatch] 分组补写后仍缺 {} 行", args.size() - rows);
            }
            return rows;
        } catch (Exception ex) {
            if (metrics != null) {
                metrics.recordPersistenceFailure("engine-batch", System.nanoTime() - startNanos);
            }
            log.warn("[Persist-EngineBatch] 分组写入异常 ({} 行): {}",
                    args.size(), ex.getMessage());
            return 0;
        }
    }

    private static int countBatchSuccess(int[] counts, int expected) {
        if (counts == null || counts.length != expected) {
            return -1;
        }
        int rows = 0;
        for (int c : counts) {
            if (c == java.sql.Statement.EXECUTE_FAILED) {
                return -1;
            }
            rows += (c == java.sql.Statement.SUCCESS_NO_INFO) ? 1 : Math.max(0, c);
        }
        return rows == expected ? rows : -1;
    }

    private static void rollbackQuietly(java.sql.Connection con) {
        try {
            con.rollback();
        } catch (Exception ignored) {
            // 回滚本身失败只记录，上层补写仍可推进
        }
    }

    private int writeEngineRowsFallback(List<Object[]> args) {
        int rows = 0;
        for (Object[] arg : args) {
            boolean ok = false;
            for (int attempt = 0; attempt < 2 && !ok; attempt++) {
                try {
                    ok = jdbcTemplate.update(ENGINE_BATCH_SQL, arg) == 1;
                } catch (Exception retryEx) {
                    if (attempt == 1) {
                        log.warn("[Persist-EngineBatch] 单行补写两次均失败: {}",
                                retryEx.getMessage());
                    }
                }
            }
            if (ok) {
                rows++;
            }
        }
        return rows;
    }

    /**
     * 批量写入的异步入口：整批占用<b>一个</b> {@code persistenceExecutor} 任务，
     * 与单行 {@code saveEngine} 共用同一线程池与同一降级语义（CallerRuns）。
     */
    @Async("persistenceExecutor")
    public void saveEngineBatchAsync(List<EnginePoint> batch) {
        saveEngineBatch(batch);
    }
}
