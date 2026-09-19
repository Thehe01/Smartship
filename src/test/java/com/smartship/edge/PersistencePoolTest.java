package com.smartship.edge;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.pool.MonitoredCallerRunsPolicy;
import com.smartship.edge.routing.pool.PersistenceAsyncConfig;
import com.smartship.edge.routing.pool.PersistencePoolMetrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PersistencePoolTest {

    @Test
    @DisplayName("测试 persistenceExecutor 核心配额与命名线程治理")
    void testExecutorConfigurationAndThreadNaming() throws InterruptedException {
        EdgeProperties properties = new EdgeProperties();
        PersistenceAsyncConfig config = new PersistenceAsyncConfig(properties);
        MonitoredCallerRunsPolicy policy = config.persistenceRejectionPolicy();
        ThreadPoolTaskExecutor executor = config.persistenceExecutor(policy);

        try {
            assertEquals(2, executor.getCorePoolSize(), "核心线程数必须为 2");
            assertEquals(4, executor.getMaxPoolSize(), "最大线程数必须为 4");
            assertEquals(500, executor.getQueueCapacity(), "有界任务队列深度必须为 500");

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> workerThreadName = new AtomicReference<>();

            executor.submit(() -> {
                workerThreadName.set(Thread.currentThread().getName());
                latch.countDown();
            });

            assertTrue(latch.await(3, TimeUnit.SECONDS), "异步任务应在 3 秒内执行完毕");
            assertNotNull(workerThreadName.get());
            assertTrue(workerThreadName.get().startsWith("persistence-worker-"),
                    "工作线程名必须以 'persistence-worker-' 开头，当前为: " + workerThreadName.get());
        } finally {
            executor.destroy();
        }
    }

    @Test
    @DisplayName("测试 PersistencePoolMetrics 指标快照与运行状态采集")
    void testPoolMetricsSnapshot() throws InterruptedException {
        EdgeProperties properties = new EdgeProperties();
        PersistenceAsyncConfig config = new PersistenceAsyncConfig(properties);
        MonitoredCallerRunsPolicy policy = config.persistenceRejectionPolicy();
        ThreadPoolTaskExecutor executor = config.persistenceExecutor(policy);
        PersistencePoolMetrics metrics = new PersistencePoolMetrics(executor, policy);

        try {
            assertEquals(0, metrics.getActiveCount());
            assertEquals(0, metrics.getQueueSize());
            assertEquals(0, metrics.getRejectCount());

            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch taskStartedLatch = new CountDownLatch(2);

            // 提交 2 个长时间运行的任务占满核心线程
            for (int i = 0; i < 2; i++) {
                executor.submit(() -> {
                    taskStartedLatch.countDown();
                    try {
                        blockLatch.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    }
                });
            }

            assertTrue(taskStartedLatch.await(3, TimeUnit.SECONDS));
            assertEquals(2, metrics.getActiveCount(), "此时应有 2 个活跃工作线程正在执行");

            // 提交 3 个排队任务
            for (int i = 0; i < 3; i++) {
                executor.submit(() -> {
                    try {
                        blockLatch.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    }
                });
            }

            PersistencePoolMetrics.Snapshot snapshot = metrics.getSnapshot();
            assertNotNull(snapshot);
            assertEquals(2, snapshot.activeThreads());
            assertEquals(3, snapshot.queueSize());
            assertEquals(500, snapshot.queueCapacity());
            assertEquals(497, snapshot.queueRemainingCapacity());

            // 释放任务
            blockLatch.countDown();
            Thread.sleep(100);

            assertTrue(metrics.getCompletedTaskCount() >= 2, "任务应已陆续完成");
        } finally {
            executor.destroy();
        }
    }

    @Test
    @DisplayName("测试队列满载触发 MonitoredCallerRunsPolicy 降级反压与拒绝计数统计")
    void testCallerRunsBackpressureAndRejectionCount() throws InterruptedException {
        // 创建一个小容量线程池精确验证 CallerRuns 行为：core=1, max=1, queue=2
        ThreadPoolTaskExecutor smallExecutor = new ThreadPoolTaskExecutor();
        MonitoredCallerRunsPolicy policy = new MonitoredCallerRunsPolicy();
        smallExecutor.setCorePoolSize(1);
        smallExecutor.setMaxPoolSize(1);
        smallExecutor.setQueueCapacity(2);
        smallExecutor.setThreadNamePrefix("test-worker-");
        smallExecutor.setRejectedExecutionHandler(policy);
        smallExecutor.initialize();

        try {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch taskStarted = new CountDownLatch(1);

            // 任务 1：占用唯一工作线程
            smallExecutor.submit(() -> {
                taskStarted.countDown();
                try {
                    blockLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
            });

            assertTrue(taskStarted.await(2, TimeUnit.SECONDS));

            // 任务 2 和 任务 3：填满容量为 2 的队列
            smallExecutor.submit(() -> {});
            smallExecutor.submit(() -> {});

            // 此时核心线程占用，队列已满 (2/2)
            // 提交第 4 个任务：必须触发 MonitoredCallerRunsPolicy 降级在调用线程直接同步执行！
            AtomicBoolean executedByCaller = new AtomicBoolean(false);
            String callerThreadName = Thread.currentThread().getName();

            assertDoesNotThrow(() -> {
                smallExecutor.submit(() -> {
                    if (Thread.currentThread().getName().equals(callerThreadName)) {
                        executedByCaller.set(true);
                    }
                });
            }, "CallerRunsPolicy 绝不能抛出 RejectedExecutionException");

            assertTrue(executedByCaller.get(), "溢出任务必须由调用方当前线程同步执行，形成背压");
            assertEquals(1, policy.getRejectCount(), "拒绝计数必须精准统计到 1 次降级");

            blockLatch.countDown();
        } finally {
            smallExecutor.destroy();
        }
    }
}
