package com.smartship.edge.benchmark.simulator;

import com.smartship.edge.uploader.mqtt.MqttPublisher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 故障注入 MQTT 网关（测试作用域，offline-capable，确定性，不依赖公网 broker）。
 * <p>
 * 基于 Mockito 构造 {@link MqttPublisher} 行为脚本：每次 publish 都会用真实
 * {@link MqttPublisher#stableMessageId} 计算并记录 msg_id，再按当前模式返回成功/失败。
 * 语义与生产一致：失败由上游 {@code DatabaseUploadPoller} 触发 break 冻结游标。
 */
public final class FaultInjectingMqttGateway {

    public enum Mode {
        /** 前 N 条成功，之后全部失败（模拟断网） */
        SUCCEED_FIRST_N_THEN_FAIL,
        /** 全部成功（模拟恢复） */
        ALWAYS_SUCCEED,
        /** 全部失败（模拟持续离线） */
        ALWAYS_FAIL
    }

    public record Published(String mmsi, String type, Map<String, Object> row, String msgId) {
    }

    private final AtomicReference<Mode> mode = new AtomicReference<>(Mode.ALWAYS_SUCCEED);
    private final AtomicInteger succeedBudget = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger attempts = new AtomicInteger();
    private final AtomicInteger successes = new AtomicInteger();
    private final List<Published> published = new CopyOnWriteArrayList<>();
    private final MqttPublisher publisher;

    public FaultInjectingMqttGateway() {
        MqttPublisher mock = mock(MqttPublisher.class);
        when(mock.publish(anyString(), anyString(), anyString(), anyMap())).thenAnswer(inv -> {
            String mmsi = inv.getArgument(0);
            String type = inv.getArgument(1);
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) inv.getArgument(3);
            String msgId = MqttPublisher.stableMessageId(mmsi, type, row);
            // 注意：业务行含大量 null 列（H2 SELECT *），必须用允许 null 的拷贝，
            // Map.copyOf 会抛 NPE（这正是本次 benchmark 自己踩出的坑，已修复）
            published.add(new Published(mmsi, type,
                    java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(row)), msgId));
            attempts.incrementAndGet();
            Mode m = mode.get();
            boolean ok = switch (m) {
                case ALWAYS_SUCCEED -> true;
                case ALWAYS_FAIL -> false;
                case SUCCEED_FIRST_N_THEN_FAIL -> successes.get() < succeedBudget.get();
            };
            if (ok) {
                successes.incrementAndGet();
            }
            return ok;
        });
        this.publisher = mock;
    }

    public MqttPublisher publisher() {
        return publisher;
    }

    public void succeedFirstNThenFail(int n) {
        succeedBudget.set(n);
        mode.set(Mode.SUCCEED_FIRST_N_THEN_FAIL);
    }

    public void alwaysSucceed() {
        mode.set(Mode.ALWAYS_SUCCEED);
    }

    public void alwaysFail() {
        mode.set(Mode.ALWAYS_FAIL);
    }

    public int attempts() {
        return attempts.get();
    }

    public int successes() {
        return successes.get();
    }

    public List<Published> published() {
        return List.copyOf(published);
    }

    /** 按业务 id 查找某行的历次发布记录（验证重传 msg_id 稳定）。 */
    public List<Published> publishedForId(Object id) {
        return published.stream()
                .filter(p -> id == null ? p.row().get("id") == null
                        : id.toString().equals(String.valueOf(p.row().get("id"))))
                .toList();
    }
}
