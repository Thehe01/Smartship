package com.smartship.edge.routing.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 按 key 分组的定量攒批器（无定时线程，零生命周期管理）。
 * <p>
 * 语义：同 key 的行攒满 {@code batchSize} 即触发一次 {@code flush} 回调，
 * 调用方把整批作为<b>一个</b>持久化任务提交（一次 {@code batchUpdate}），
 * 从而把任务数从 N 行降到 N/batchSize 级。尾批由调用方在周期末/关闭时
 * 调用 {@link #drainAll(Consumer)} 显式刷出，不丢数据是调用方的责任，
 * 本类只保证：同 key 先后顺序、满批恰好触发一次、无锁化并发 {@code add}。
 * <p>
 * 线程安全：key 级互斥（stable per-key 锁，不同 key 零竞争）。
 * 锁表随 key 基数增长——船数即 key 上界（注册表有界），内存可忽略。
 */
public class WriteBatcher<K, T> {

    private final int batchSize;
    private final ConcurrentHashMap<K, ArrayList<T>> buffers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<K, Object> locks = new ConcurrentHashMap<>();

    public WriteBatcher(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.batchSize = batchSize;
    }

    /**
     * 追加一行；若该 key 恰好攒满一批，同步回调 {@code flush} 并返回刷出的行数，
     * 否则返回 0。flush 在调用线程执行——生产调用方应在回调内把整批提交为
     * 单个异步任务，而不是在采集线程直接打 DB。
     */
    public int add(K key, T row, Consumer<List<T>> flush) {
        ArrayList<T> full;
        synchronized (lockOf(key)) {
            ArrayList<T> buf = buffers.computeIfAbsent(key, k -> new ArrayList<>());
            buf.add(row);
            if (buf.size() >= batchSize) {
                full = buffers.remove(key);
            } else {
                return 0;
            }
        }
        flush.accept(full);
        return full.size();
    }

    /** 当前缓存的总行数（所有 key 之和，仅供监控/测试）。 */
    public int bufferedRows() {
        int n = 0;
        for (K key : buffers.keySet()) {
            synchronized (lockOf(key)) {
                ArrayList<T> buf = buffers.get(key);
                if (buf != null) {
                    n += buf.size();
                }
            }
        }
        return n;
    }

    /**
     * 刷出全部尾批：每个非空 key 回调一次 {@code flush}，返回刷出的总行数。
     * 调用后内部缓存为空。
     */
    public int drainAll(Consumer<List<T>> flush) {
        Map<K, ArrayList<T>> snapshot = new LinkedHashMap<>();
        for (K key : buffers.keySet()) {
            synchronized (lockOf(key)) {
                ArrayList<T> buf = buffers.remove(key);
                if (buf != null && !buf.isEmpty()) {
                    snapshot.put(key, buf);
                }
            }
        }
        int n = 0;
        for (ArrayList<T> batch : snapshot.values()) {
            flush.accept(batch);
            n += batch.size();
        }
        return n;
    }

    private Object lockOf(K key) {
        return locks.computeIfAbsent(key, k -> new Object());
    }
}
