package com.smartship.edge.routing.pool;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 异步持久化线程池可观测性指标组件
 * <p>
 * 为监控告警、状态探针及后续 P1-3 (Prometheus / Micrometer) 提供精准的基础指标接口：
 * - 活跃工作线程数 (Active Threads)
 * - 队列堆积深度 (Queue Size & Remaining Capacity)
 * - 已完成任务总数 (Completed Tasks)
 * - 触发降级反压次数 (Reject / Backpressure Count)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PersistencePoolMetrics {

    @Qualifier("persistenceExecutor")
    private final ThreadPoolTaskExecutor persistenceExecutor;
    private final MonitoredCallerRunsPolicy rejectionPolicy;

    /**
     * 获取当前正在执行持久化落库的活跃工作线程数
     */
    public int getActiveCount() {
        return persistenceExecutor.getActiveCount();
    }

    /**
     * 获取当前线程池内工作线程实际存活总数
     */
    public int getPoolSize() {
        return persistenceExecutor.getPoolSize();
    }

    /**
     * 获取核心线程数配额
     */
    public int getCorePoolSize() {
        return persistenceExecutor.getCorePoolSize();
    }

    /**
     * 获取最大线程数配额
     */
    public int getMaxPoolSize() {
        return persistenceExecutor.getMaxPoolSize();
    }

    /**
     * 获取当前有界队列中排队等待落库的任务数
     */
    public int getQueueSize() {
        ThreadPoolExecutor tpe = persistenceExecutor.getThreadPoolExecutor();
        return tpe != null ? tpe.getQueue().size() : 0;
    }

    /**
     * 获取当前有界队列剩余可容纳任务数
     */
    public int getQueueRemainingCapacity() {
        ThreadPoolExecutor tpe = persistenceExecutor.getThreadPoolExecutor();
        return tpe != null ? tpe.getQueue().remainingCapacity() : 0;
    }

    /**
     * 获取线程池已完成执行任务总数
     */
    public long getCompletedTaskCount() {
        ThreadPoolExecutor tpe = persistenceExecutor.getThreadPoolExecutor();
        return tpe != null ? tpe.getCompletedTaskCount() : 0;
    }

    /**
     * 获取历史上提交给线程池的总任务数
     */
    public long getTotalTaskCount() {
        ThreadPoolExecutor tpe = persistenceExecutor.getThreadPoolExecutor();
        return tpe != null ? tpe.getTaskCount() : 0;
    }

    /**
     * 获取因队列满载而触发 CallerRuns 降级反压的次数
     */
    public long getRejectCount() {
        return rejectionPolicy.getRejectCount();
    }

    /**
     * 获取只读不可变的线程池全量指标瞬时快照
     */
    public Snapshot getSnapshot() {
        return new Snapshot(
                getActiveCount(),
                getPoolSize(),
                getCorePoolSize(),
                getMaxPoolSize(),
                getQueueSize(),
                persistenceExecutor.getQueueCapacity(),
                getQueueRemainingCapacity(),
                getCompletedTaskCount(),
                getTotalTaskCount(),
                getRejectCount()
        );
    }

    /**
     * 线程池指标快照 Record
     */
    public record Snapshot(
            int activeThreads,
            int currentPoolSize,
            int corePoolSize,
            int maxPoolSize,
            int queueSize,
            int queueCapacity,
            int queueRemainingCapacity,
            long completedTasks,
            long totalTasks,
            long rejectCount
    ) {
        @Override
        public String toString() {
            return String.format(
                    "PersistencePoolSnapshot[active=%d/%d, poolSize=%d, queue=%d/%d, completed=%d, rejects=%d]",
                    activeThreads, maxPoolSize, currentPoolSize, queueSize, queueCapacity, completedTasks, rejectCount
            );
        }
    }
}
