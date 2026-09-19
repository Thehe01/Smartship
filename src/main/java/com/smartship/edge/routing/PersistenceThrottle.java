package com.smartship.edge.routing;

import com.smartship.edge.config.EdgeProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 写入节流防抖器（无锁 CAS 实现）
 * <p>
 * 核心并发设计：
 * 1. 针对工业传感器高达 10Hz~50Hz 的高频发射，按 "MMSI + 业务类型" 粒度实施最小写入间隔（默认 1 秒）
 * 2. 基于 ConcurrentHashMap 与 AtomicLong 的无锁 CAS 自旋设计，零重锁竞争，纳秒级快路径短路过滤
 * 3. 削减 80% 以上冗余磁盘 I/O 写入，保护边缘工控机闪存寿命并避免锁争用
 */
@Component
@RequiredArgsConstructor
public class PersistenceThrottle {

    private final EdgeProperties properties;
    private final ConcurrentHashMap<String, AtomicLong> lastWriteTimes = new ConcurrentHashMap<>();

    public boolean shouldWrite(String key) {
        long intervalMs = Math.max(0, properties.getCollect().getPersist().getMinWriteIntervalSeconds()) * 1000L;
        if (intervalMs <= 0) {
            return true;
        }

        long now = System.currentTimeMillis();
        AtomicLong last = lastWriteTimes.computeIfAbsent(key, k -> new AtomicLong(0));
        while (true) {
            long prev = last.get();
            // 1. 快路径（Fast-Path）：时间差不足，直接短路返回 false，无锁竞争
            if (prev > 0 && now - prev < intervalMs) {
                return false;
            }
            // 2. CAS 原子更新尝试：成功则抢占到本次落库资格
            if (last.compareAndSet(prev, now)) {
                return true;
            }
            // 3. 并发竞争冲突，进入下一轮 while 自旋
        }
    }

    /**
     * 重置指定 Key（主要用于单测或船舶重置）
     */
    public void reset(String key) {
        lastWriteTimes.remove(key);
    }
}
