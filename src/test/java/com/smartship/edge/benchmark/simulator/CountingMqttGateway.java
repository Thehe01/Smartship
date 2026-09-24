package com.smartship.edge.benchmark.simulator;

import com.smartship.edge.uploader.mqtt.MqttPublisher;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 大积压补传计数网关（测试作用域）。
 * <p>
 * 与 {@link FaultInjectingMqttGateway} 的区别：不保留全量发布记录
 * （75 万行全量记录会撑爆内存且 CopyOnWriteArrayList 追加退化为 O(n)），
 * 只记录：attempts/successes 计数、按业务 id 的 {@link BitSet} 覆盖、
 * 采样 id 的历次 msg_id（验证重传稳定性）。行为脚本语义与生产一致。
 */
public final class CountingMqttGateway {

    public enum Mode {
        SUCCEED_FIRST_N_THEN_FAIL,
        ALWAYS_SUCCEED,
        ALWAYS_FAIL
    }

    private final AtomicReference<Mode> mode = new AtomicReference<>(Mode.ALWAYS_SUCCEED);
    private final AtomicInteger succeedBudget = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger attempts = new AtomicInteger();
    private final AtomicInteger successes = new AtomicInteger();
    private final BitSet covered;
    private final long expectedRows;
    private final long[] sampleIds;
    private final List<String>[] sampleMsgIds;
    private final MqttPublisher publisher;

    @SuppressWarnings("unchecked")
    public CountingMqttGateway(long expectedRows, long... sampleIds) {
        if (expectedRows <= 0 || expectedRows > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("expectedRows out of range");
        }
        this.expectedRows = expectedRows;
        this.covered = new BitSet((int) expectedRows + 1);
        this.sampleIds = sampleIds.clone();
        this.sampleMsgIds = new List[sampleIds.length];
        for (int i = 0; i < sampleIds.length; i++) {
            sampleMsgIds[i] = java.util.Collections.synchronizedList(new ArrayList<>());
        }
        MqttPublisher mock = mock(MqttPublisher.class);
        when(mock.publish(anyString(), anyString(), anyString(), anyMap())).thenAnswer(inv -> {
            String mmsi = inv.getArgument(0);
            String type = inv.getArgument(1);
            Map<String, Object> row = inv.getArgument(3);
            String msgId = MqttPublisher.stableMessageId(mmsi, type, row);
            attempts.incrementAndGet();
            Object idObj = row.get("id");
            long id = (idObj instanceof Number num) ? num.longValue() : Long.parseLong(String.valueOf(idObj));
            if (id >= 1 && id <= expectedRows) {
                synchronized (covered) {
                    covered.set((int) id);
                }
            }
            for (int i = 0; i < this.sampleIds.length; i++) {
                if (this.sampleIds[i] == id) {
                    sampleMsgIds[i].add(msgId);
                }
            }
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

    /** 已发布过的互异业务 id 数（补传覆盖率，满额即零丢失）。 */
    public int covered() {
        synchronized (covered) {
            return covered.cardinality();
        }
    }

    /** 采样 id 的历次 msg_id（验证重传恒定）。 */
    public List<String> msgIdsForSample(long id) {
        for (int i = 0; i < sampleIds.length; i++) {
            if (sampleIds[i] == id) {
                return List.copyOf(sampleMsgIds[i]);
            }
        }
        throw new IllegalArgumentException("not a sampled id: " + id);
    }
}
