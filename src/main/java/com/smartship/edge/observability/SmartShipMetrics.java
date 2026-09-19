package com.smartship.edge.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 边端核心可观测性业务指标统一管理组件
 * <p>
 * 统一管理持久化落库、MQTT 网络传输与 Uploader 增量推送的 Counter 与 Timer 指标。
 * 严格执行有限枚举低基数约束，杜绝 MMSI、shipId、msgId、动态 SQL 表名等高基数 Label。
 */
@Component
public class SmartShipMetrics {

    private static final Set<String> ALLOWED_TELEMETRY_TYPES = Set.of(
            "gps", "wind", "depth", "rudder", "engine"
    );

    private final MeterRegistry registry;

    // 预热/缓存 Counter 和 Timer，避免重复构建 MeterId 产生的开销
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();

    public SmartShipMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public MeterRegistry getRegistry() {
        return registry;
    }

    /**
     * 规范化 telemetry type / stream 标签，确保基数有限
     */
    public static String normalizeType(String type) {
        if (type == null) {
            return "unknown";
        }
        String clean = type.toLowerCase().trim();
        if (clean.startsWith("nmea_")) {
            clean = clean.substring(5);
        }
        return ALLOWED_TELEMETRY_TYPES.contains(clean) ? clean : "other";
    }

    // ================= Persistence 指标 =================

    public void recordPersistenceSuccess(String type, long durationNanos) {
        String cleanType = normalizeType(type);
        getOrCreateCounter("smartship_persistence_writes_total", "type", cleanType, "result", "success")
                .increment();
        if (durationNanos >= 0) {
            getOrCreateTimer("smartship_persistence_write_duration_seconds", "type", cleanType)
                    .record(durationNanos, TimeUnit.NANOSECONDS);
        }
    }

    public void recordPersistenceFailure(String type, long durationNanos) {
        String cleanType = normalizeType(type);
        getOrCreateCounter("smartship_persistence_writes_total", "type", cleanType, "result", "failure")
                .increment();
        if (durationNanos >= 0) {
            getOrCreateTimer("smartship_persistence_write_duration_seconds", "type", cleanType)
                    .record(durationNanos, TimeUnit.NANOSECONDS);
        }
    }

    // ================= MQTT 指标 =================

    public void recordMqttConnect(boolean success) {
        getOrCreateCounter("smartship_mqtt_connect_total", "result", success ? "success" : "failure")
                .increment();
    }

    public void recordMqttPublish(boolean success, long durationNanos) {
        getOrCreateCounter("smartship_mqtt_publish_total", "result", success ? "success" : "failure")
                .increment();
        if (durationNanos >= 0) {
            getOrCreateTimer("smartship_mqtt_publish_duration_seconds")
                    .record(durationNanos, TimeUnit.NANOSECONDS);
        }
    }

    public void recordMqttConnectionLost() {
        getOrCreateCounter("smartship_mqtt_connection_lost_total")
                .increment();
    }

    public void recordMqttReconnect() {
        getOrCreateCounter("smartship_mqtt_reconnect_total")
                .increment();
    }

    // ================= Uploader 指标 =================

    public void recordUploadRows(String stream, boolean success, long count) {
        String cleanStream = normalizeType(stream);
        getOrCreateCounter("smartship_uploader_rows_total", "stream", cleanStream, "result", success ? "success" : "failure")
                .increment(count);
    }

    public void recordUploadBatch(String stream, boolean success) {
        String cleanStream = normalizeType(stream);
        getOrCreateCounter("smartship_uploader_batches_total", "stream", cleanStream, "result", success ? "success" : "failure")
                .increment();
    }

    // ================= 内部缓存辅助方法 =================

    private Counter getOrCreateCounter(String name, String... tags) {
        String key = buildKey(name, tags);
        return counters.computeIfAbsent(key, k -> Counter.builder(name)
                .tags(tags)
                .register(registry));
    }

    private Timer getOrCreateTimer(String name, String... tags) {
        String key = buildKey(name, tags);
        return timers.computeIfAbsent(key, k -> Timer.builder(name)
                .tags(tags)
                .register(registry));
    }

    private String buildKey(String name, String... tags) {
        if (tags == null || tags.length == 0) {
            return name;
        }
        StringBuilder sb = new StringBuilder(name);
        for (int i = 0; i < tags.length; i += 2) {
            sb.append('|').append(tags[i]).append('=').append(tags[i + 1]);
        }
        return sb.toString();
    }
}
