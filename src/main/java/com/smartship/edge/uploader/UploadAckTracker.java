package com.smartship.edge.uploader;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Application ACK 跟踪器：PUBACK 置 IN_FLIGHT，KAFKA_COMMITTED 置 ACKED，
 * 游标只推进连续 ACK watermark。
 *
 * <p>语义（与岸端 {@code MqttAckPublisher} 镜像）：
 * <ul>
 *   <li>PUBACK 只表示 Broker 已接收——记为 {@code IN_FLIGHT}，<b>不推进游标</b>；</li>
 *   <li>收到匹配 {@code msg_id}（及 {@code seq}，如有）的 ACK 才记
 *   {@code ACKED}；游标推进到“自 base 起连续 ACK 的最大值”，乱序 ACK 只记账不推进；</li>
 *   <li>ACK 可能丢失或重复（QoS1），绝不凭空产生：超时未 ACK 的进入补发集合，
 *   由 poller 重新发布（msg_id 稳定，岸端 {@code UNIQUE} 吸收重复）；</li>
 *   <li>重启恢复天然成立：游标表是持久化的，未 ACK 的行（id &gt; 游标）下次轮询
 *   照常查出重发，msg_id 由业务键确定故与之前完全相同。</li>
 * </ul>
 *
 * <p>键空间为（mmsi，表名），与游标表粒度一致；另有 msg_id 全局二级索引供 ACK
 * 分发（msg_id 全局唯一，含 mmsi 指纹）。非线程安全——调用方（poller 定时线程
 * + MQTT 回调线程）需外部同步；本类只保证单线程下的确定性。
 */
@Slf4j
public class UploadAckTracker {

    public enum State {
        IN_FLIGHT,
        ACKED
    }

    public record Tracked(long rowId, String msgId, State state, long sentAtMs) {
        Tracked acked() {
            return new Tracked(rowId, msgId, State.ACKED, sentAtMs);
        }

        Tracked resent(long nowMs) {
            return new Tracked(rowId, msgId, State.IN_FLIGHT, nowMs);
        }
    }

    private record EntryRef(String key, long rowId) {
    }

    private final Supplier<Long> clock;
    private final Map<String, TreeMap<Long, Tracked>> tables = new HashMap<>();
    private final Map<String, EntryRef> byMsgId = new HashMap<>();
    /** 每表见过的最大行号：判定“空洞可跳过”的依据（见 watermark）。 */
    private final Map<String, Long> seenHigh = new HashMap<>();
    /** 每表已验证连续的最大值：watermark 单调的内存证据（重启以 DB 游标为准）。 */
    private final Map<String, Long> highMarks = new HashMap<>();

    public UploadAckTracker() {
        this(System::currentTimeMillis);
    }

    /** 测试用时钟注入：时间推进完全可控，不靠 sleep 猜超时。 */
    public UploadAckTracker(Supplier<Long> clock) {
        this.clock = clock;
    }

    private static String key(String mmsi, String table) {
        return mmsi + '\0' + table;
    }

    private TreeMap<Long, Tracked> table(String mmsi, String tableName) {
        return tables.computeIfAbsent(key(mmsi, tableName), k -> new TreeMap<>());
    }

    /**
     * PUBACK 成功后登记在途。若同一行已在途（重发），刷新发送时间戳；
     * 若已 ACK（ACK 先到、PUBACK 循环迟到之类乱序），保持 ACKED 不动。
     */
    public void track(String mmsi, String tableName, long rowId, String msgId) {
        trackAt(mmsi, tableName, rowId, msgId, clock.get());
    }

    void trackAt(String mmsi, String tableName, long rowId, String msgId, long nowMs) {
        String k = key(mmsi, tableName);
        TreeMap<Long, Tracked> rows = table(mmsi, tableName);
        Tracked existing = rows.get(rowId);
        if (existing != null && existing.state() == State.ACKED) {
            return;
        }
        rows.put(rowId, new Tracked(rowId, msgId, State.IN_FLIGHT, nowMs));
        byMsgId.put(msgId, new EntryRef(k, rowId));
        seenHigh.merge(k, rowId, Math::max);
    }

    /**
     * 收到 Application ACK：msg_id 必须命中，未知/已清理的 ACK 忽略（迟到的重复
     * ACK）；seq 如有则必须与登记行号一致，否则忽略（错位保护）。
     *
     * @return true 表示命中了一条在途记录并标记 ACKED
     */
    public boolean onAck(String mmsi, String msgId, String seq) {
        EntryRef ref = byMsgId.get(msgId);
        if (ref == null) {
            log.debug("[Uploader-ACK] 未知 ACK 忽略: mmsi={}, msg_id={}", mmsi, msgId);
            return false;
        }
        TreeMap<Long, Tracked> rows = tables.get(ref.key());
        if (rows == null) {
            return false;
        }
        Tracked tracked = rows.get(ref.rowId());
        if (tracked == null || tracked.state() == State.ACKED) {
            return false;
        }
        if (seq != null && !seq.isBlank()
                && !seq.trim().equals(String.valueOf(tracked.rowId()))) {
            log.warn("[Uploader-ACK] seq 错位忽略: msg_id={}, 期望行={}, ACK行={}",
                    msgId, tracked.rowId(), seq);
            return false;
        }
        rows.put(tracked.rowId(), tracked.acked());
        return true;
    }

    /**
     * 连续 ACK watermark：自游标（或已验证高位）起连续 ACK 的最大值。
     *
     * <p>行号不必从游标+1 开始（如首批行号 1001 起而游标为 0）：从未登记且
     * 不大于见过最大值的行号视为空洞（若存在早被查出登记）直接跳过；超出见过
     * 范围或遇到未 ACK 即停。推进后裁剪已确认前缀（两份索引同步清理）。
     */
    public long watermark(String mmsi, String tableName, long cursorBase) {
        String k = key(mmsi, tableName);
        TreeMap<Long, Tracked> rows = tables.get(k);
        if (rows == null || rows.isEmpty()) {
            return Math.max(cursorBase, highMarks.getOrDefault(k, cursorBase));
        }
        long seen = seenHigh.getOrDefault(k, cursorBase);
        long w = Math.max(cursorBase, highMarks.getOrDefault(k, cursorBase));
        long id = w + 1;
        long first = rows.firstKey();
        if (first > id && first - 1 <= seen) {
            id = first;
        }
        while (true) {
            Tracked next = rows.get(id);
            if (next == null) {
                if (id <= seen) {
                    id++;
                    continue;
                }
                break;
            }
            if (next.state() != State.ACKED) {
                break;
            }
            w = id;
            id++;
        }
        if (w > highMarks.getOrDefault(k, Long.MIN_VALUE)) {
            highMarks.put(k, w);
            var head = rows.headMap(w, true);
            for (Tracked t : new ArrayList<>(head.values())) {
                if (t.state() == State.ACKED) {
                    head.remove(t.rowId());
                    byMsgId.remove(t.msgId(), new EntryRef(k, t.rowId()));
                }
            }
        }
        return w;
    }

    /**
     * 超时未 ACK 的在途集合（升序），供 poller 补发，调用方自行限量。
     * 补发后必须调 {@link #track} 刷新时间戳，否则下次还会被捞出。
     */
    public List<Tracked> resendDue(String mmsi, String tableName, long ackTimeoutMs) {
        return resendDueAt(mmsi, tableName, ackTimeoutMs, clock.get());
    }

    List<Tracked> resendDueAt(String mmsi, String tableName, long ackTimeoutMs, long nowMs) {
        List<Tracked> due = new ArrayList<>();
        for (Tracked t : table(mmsi, tableName).values()) {
            if (t.state() == State.IN_FLIGHT && nowMs - t.sentAtMs() >= ackTimeoutMs) {
                due.add(t);
            }
        }
        return due;
    }

    /** 是否已登记（任何状态）：主循环只发新行，已在途的等 ACK 或超时补发。 */
    public boolean isTracked(String mmsi, String tableName, long rowId) {
        TreeMap<Long, Tracked> rows = tables.get(key(mmsi, tableName));
        return rows != null && rows.containsKey(rowId);
    }

    /** 放弃跟踪一行（源行已被清理，补发无意义）。 */
    public void forget(String mmsi, String tableName, long rowId) {
        TreeMap<Long, Tracked> rows = tables.get(key(mmsi, tableName));
        if (rows == null) {
            return;
        }
        Tracked removed = rows.remove(rowId);
        if (removed != null) {
            byMsgId.remove(removed.msgId());
        }
    }

    /** 在途计数：滑动窗口满则停发新消息等 ACK。 */
    public int inFlightCount(String mmsi, String tableName) {        int n = 0;
        for (Tracked t : table(mmsi, tableName).values()) {
            if (t.state() == State.IN_FLIGHT) {
                n++;
            }
        }
        return n;
    }

    /** 测试观察：当前登记总数。 */
    int trackedCount(String mmsi, String tableName) {
        return table(mmsi, tableName).size();
    }
}
