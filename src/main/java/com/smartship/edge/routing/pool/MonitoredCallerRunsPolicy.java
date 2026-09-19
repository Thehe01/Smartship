package com.smartship.edge.routing.pool;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带监控计数的 CallerRuns 降级反压拒绝策略
 * <p>
 * 当异步持久化有界队列（500）与最大线程（4）全满时：
 * 1. 若线程池仍在运行，任务交由提交线程（协议解析线程）直接同步运行，形成自然的端到端反压（Backpressure），强迫数据接收减速；
 * 2. 线程安全原子累加 rejectCount 统计降级次数，供指标采集组件与 Prometheus 实时感知边缘写入瓶颈；
 * 3. 若线程池已处于关闭状态（isShutdown 为 true），明确拒绝新任务并抛出 RejectedExecutionException，避免关闭期任务既无法执行又产生误导性计数与日志。
 */
@Slf4j
public class MonitoredCallerRunsPolicy implements RejectedExecutionHandler {

    private final AtomicLong rejectCount = new AtomicLong(0);

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (executor.isShutdown()) {
            log.error("[PersistencePool] executor 已关闭，拒绝新任务");
            throw new RejectedExecutionException("persistenceExecutor already shutdown");
        }
        long current = rejectCount.incrementAndGet();
        log.warn("[PersistencePool] 异步持久化队列已满，触发 CallerRuns 降级反压并在调用者线程同步执行: rejectCount={}", current);
        r.run();
    }

    public long getRejectCount() {
        return rejectCount.get();
    }

    public void resetRejectCount() {
        rejectCount.set(0);
    }
}
