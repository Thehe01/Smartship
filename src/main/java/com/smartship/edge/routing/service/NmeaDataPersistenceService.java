package com.smartship.edge.routing.service;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.observability.SmartShipMetrics;
import com.smartship.edge.persist.FileFallbackStore;
import com.smartship.edge.routing.PersistenceThrottle;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import com.smartship.edge.persistence.EdgeTelemetryRepository;
import com.smartship.edge.persistence.TelemetryStream;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * 边缘端时序数据持久化服务（单船单库，无分船路由）。
 *
 * <p>船端只存本船数据：通过 MyBatis Mapper 使用 Spring 默认单数据源 {@link EdgeTelemetryRepository}（本地本船库）。
 * {@code mmsi} 参数仅作为行内业务字段写入，不再做数据源路由键。
 */
@Slf4j
@Service
public class NmeaDataPersistenceService {

    private final EdgeTelemetryRepository repository;
    private final EdgeProperties properties;
    private final PersistenceThrottle throttle;
    private final SmartShipMetrics metrics;
    private final FileFallbackStore fallbackStore;

    @Autowired
    public NmeaDataPersistenceService(EdgeTelemetryRepository repository,
                                      EdgeProperties properties,
                                      PersistenceThrottle throttle,
                                      SmartShipMetrics metrics,
                                      FileFallbackStore fallbackStore) {
        this.repository = repository;
        this.properties = properties;
        this.throttle = throttle;
        this.metrics = metrics;
        this.fallbackStore = fallbackStore;
    }

    public NmeaDataPersistenceService(EdgeTelemetryRepository repository,
                                      EdgeProperties properties,
                                      PersistenceThrottle throttle,
                                      SmartShipMetrics metrics) {
        this(repository, properties, throttle, metrics, new FileFallbackStore(properties));
    }

    public NmeaDataPersistenceService(EdgeTelemetryRepository repository,
                                      EdgeProperties properties,
                                      PersistenceThrottle throttle) {
        this(repository, properties, throttle, null);
    }

    private String currentShipId() {
        String sid = properties.getShipId();
        String mmsi = properties.getMmsi();
        return (sid != null && !sid.isEmpty()) ? sid : mmsi;
    }

    /**
     * 耐久写入：主表一次 + 瞬态重试一次，重试耗尽后转三级兜底。
     *
     * <p>统一幂等键：{@code replayId} 由调用方在持久化入口生成一次（单行方法内、
     * EnginePoint 入批时），首次写入与即时重试复用同一个值。主表有
     * {@code UNIQUE(replay_id)}，因此“已提交但返回异常”的未知结果窗口会被去重吸收：
     * 任何 {@code DuplicateKeyException} 都直接视为成功。
     */
    private boolean updateDurable(
            String stream, String mmsi, String replayId, Object[] baseArgs) {
        Object[] args = withReplayId(baseArgs, replayId);
        try {
            repository.insert(stream, args);
            return true;
        } catch (org.springframework.dao.DuplicateKeyException dup) {
            log.info("[Persist-{}] 主表已存在同 replay_id，视为成功（未知结果窗口吸收）", stream);
            return true;
        } catch (Exception first) {
            try {
                repository.insert(stream, args);
                return true;
            } catch (org.springframework.dao.DuplicateKeyException dup2) {
                log.info("[Persist-{}] 重试命中同 replay_id，视为成功", stream);
                return true;
            } catch (Exception second) {
                writeFallback(stream, mmsi, replayId, baseArgs, messageOf(second));
                log.warn("[Persist-{}] 主表写入两次均失败，已转本地兜底: {}",
                        stream, second.getMessage());
                return false;
            }
        }
    }

    private static String messageOf(Exception e) {
        return e.getMessage() == null ? e.toString() : e.getMessage();
    }

    private static Object[] withReplayId(Object[] baseArgs, String replayId) {
        Object[] full = java.util.Arrays.copyOf(baseArgs, baseArgs.length + 1);
        full[baseArgs.length] = replayId;
        return full;
    }

    /** 持久化入口统一幂等键：调用方生成一次，全链路复用，禁止下游重新生成。 */
    static String newReplayId() {
        return java.util.UUID.randomUUID().toString();
    }

    private void writeFallback(
            String stream, String mmsi, String replayId, Object[] baseArgs, String error) {
        // tier 1：同库兜底表（可查询、可审计；payload 存可回放 JSON，回放器会重写入主表）。
        // 兜底写入同样重试一次；命中 replay_id 唯一约束说明已兜底，直接返回不再进 spool。
        Exception last = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                String payload = truncate(
                        FileFallbackStore.argsToJson(stream, mmsi, replayId, baseArgs), 65535);
                String err = truncate(error, 500);
                repository.insertFallback(new Object[]{stream, mmsi, replayId, payload, err});
                if (metrics != null) {
                    metrics.recordPersistenceFallback(stream);
                }
                return;
            } catch (org.springframework.dao.DuplicateKeyException dup) {
                log.info("[Persist-{}] 兜底表已存在同 replay_id，不再进 spool", stream);
                if (metrics != null) {
                    metrics.recordPersistenceFallback(stream);
                }
                return;
            } catch (Exception e) {
                last = e;
            }
        }
        log.warn("[Persist-{}] 兜底表亦不可写（MySQL 可能整体故障），转磁盘 spool: {}",
                stream, last != null ? last.getMessage() : "");
        // tier 2：磁盘 spool（MySQL 全挂也丢不了，回放器稍后重放；复用同一个 replayId）
        try {
            long dropped = fallbackStore.spool(stream, mmsi, replayId, baseArgs);
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

    /** Shared allowlist used by the MyBatis provider and the durable replay format. */
    public static boolean supportsStream(String stream) {
        try {
            TelemetryStream.fromKey(stream);
            return true;
        } catch (IllegalArgumentException unknown) {
            return false;
        }
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
        String replayId = newReplayId();
        if (updateDurable("gps", mmsi, replayId, args)) {
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
        String replayId = newReplayId();
        if (updateDurable("wind", mmsi, replayId, args)) {
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
        String replayId = newReplayId();
        if (updateDurable("depth", mmsi, replayId, args)) {
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
        String replayId = newReplayId();
        if (updateDurable("rudder", mmsi, replayId, args)) {
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
        String replayId = newReplayId();
        if (updateDurable("engine", mmsi, replayId, args)) {
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
            // 入批时的 replayId 全程复用：batch、逐行 retry、兜底、回放、上传 msg_id 同键。
            args.add(withReplayId(engineArgs(shipId, p), p.replayId()));
        }
        long startNanos = System.nanoTime();
        try {
            int rows = repository.insertEngineBatch(args);
            if (metrics != null) {
                metrics.recordPersistenceSuccess("engine-batch", System.nanoTime() - startNanos);
            }
            return rows;
        } catch (Exception ex) {
            if (metrics != null) {
                metrics.recordPersistenceFailure("engine-batch", System.nanoTime() - startNanos);
            }
            // TransactionTemplate has completed rollback before per-row retries begin.
            log.debug("[Persist-EngineBatch] 整批回滚转逐行补写 ({} 行): {}",
                    args.size(), ex.getMessage());
            return writeEngineRowsFallback(group, args);
        }
    }

    /**
     * 逐行补写：每行最多两次，第二次仍失败则走与单行路径完全相同的三级兜底
     * （DB 兜底表 → 磁盘 spool → 回放），而不是只记日志。
     * 调用前整批已显式回滚，无半批残留，因此补写不会产生重复行。
     */
    private int writeEngineRowsFallback(List<EnginePoint> group, List<Object[]> args) {
        int rows = 0;
        for (int i = 0; i < args.size(); i++) {
            Object[] arg = args.get(i);
            EnginePoint point = group.get(i);
            boolean ok = false;
            String lastError = "";
            for (int attempt = 0; attempt < 2 && !ok; attempt++) {
                try {
                    ok = repository.insert("engine", arg) == 1;
                } catch (org.springframework.dao.DuplicateKeyException dup) {
                    // 已提交但返回异常的未知结果窗口：同 replay_id 已在，直接成功。
                    log.info("[Persist-EngineBatch] 补写命中同 replay_id，视为成功");
                    ok = true;
                } catch (Exception retryEx) {
                    lastError = retryEx.getMessage();
                    if (attempt == 1) {
                        log.warn("[Persist-EngineBatch] 单行补写两次均失败，转三级兜底: {}",
                                lastError);
                    }
                }
            }
            if (ok) {
                rows++;
            } else {
                writeFallback("engine", point.mmsi(), point.replayId(),
                        engineArgs(shipIdOf(point), point), lastError);
            }
        }
        return rows;
    }

    private String shipIdOf(EnginePoint point) {
        String sid = properties.getShipId();
        return (sid != null && !sid.isEmpty()) ? sid : point.mmsi();
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
