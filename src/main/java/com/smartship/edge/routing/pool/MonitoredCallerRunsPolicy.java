package com.smartship.edge.routing.pool;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带监控计数的 CallerRuns 降级反压拒绝策略
 * <p>
 * 当异步持久化有界队列（500）与最大线程（4）全满时：
 * 1. 任务交由提交线程（协议解析线程）直接同步运行，形成自然的端到端反压（Backpressure），强迫数据接收减速；
 * 2. 线程安全原子累加 rejectCount 统计降级次数，供指标采集组件与 Prometheus 实时感知边缘写入瓶颈；
 * 3. 规避抛出 RejectedExecutionException 导致数据无故硬丢失。
 */
@Slf4j
public class MonitoredCallerRunsPolicy implements RejectedExecutionHandler {

    private final AtomicLong rejectCount = new AtomicLong(0);
    private final RejectedExecutionHandler delegate = new ThreadPoolExecutor.CallerRunsPolicy();

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        long current = rejectCount.incrementAndGet();
        log.warn("[PersistencePool] 异步持久化队列已满，触发 CallerRuns 降级反压并在调用者线程同步执行: rejectCount={}", current);
        delegate.rejectedExecution(r, executor);
    }

    public long getRejectCount() {
        return rejectCount.get();
    }

    public void resetRejectCount() {
        rejectCount.set(0);
    }
}
