package com.smartship.edge;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.pool.MonitoredCallerRunsPolicy;
import com.smartship.edge.routing.pool.PersistenceAsyncConfig;
import com.smartship.edge.routing.pool.PersistencePoolMetrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
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
            CountDownLatch allCompletedLatch = new CountDownLatch(5);

            // 提交 2 个长时间运行的任务占满核心线程
            for (int i = 0; i < 2; i++) {
                executor.submit(() -> {
                    taskStartedLatch.countDown();
                    try {
                        blockLatch.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    } finally {
                        allCompletedLatch.countDown();
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
                    } finally {
                        allCompletedLatch.countDown();
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
            assertTrue(allCompletedLatch.await(3, TimeUnit.SECONDS), "全部 5 个任务应在 3 秒内执行完毕");

            assertEquals(5, metrics.getCompletedTaskCount(), "5 个任务必须全部计入已完成任务数");
            assertEquals(5, metrics.getTotalTaskCount(), "总任务数必须为 5");
        } finally {
            executor.destroy();
        }
    }

    @Test
    @DisplayName("测试 completedTaskCount 在任务捕获数据库异常时仍正常累加（语义边界）")
    void testCompletedTaskCountIncrementsEvenOnDbException() throws InterruptedException {
        EdgeProperties properties = new EdgeProperties();
        PersistenceAsyncConfig config = new PersistenceAsyncConfig(properties);
        MonitoredCallerRunsPolicy policy = config.persistenceRejectionPolicy();
        ThreadPoolTaskExecutor executor = config.persistenceExecutor(policy);
        PersistencePoolMetrics metrics = new PersistencePoolMetrics(executor, policy);

        try {
            CountDownLatch latch = new CountDownLatch(1);

            // 模拟 NmeaDataPersistenceService 中落库失败但被 catch 吞掉的场景
            executor.submit(() -> {
                try {
                    throw new RuntimeException("Simulated MySQL DB connection failure");
                } catch (Exception ignored) {
                    // 业务层 catch 吞掉异常，方法正常退出
                } finally {
                    latch.countDown();
                }
            });

            assertTrue(latch.await(3, TimeUnit.SECONDS));
            assertEquals(1, metrics.getCompletedTaskCount(),
                    "即使落库发生异常被 catch，线程池层面的任务已执行完毕，completedTaskCount 必须累加");
        } finally {
            executor.destroy();
        }
    }

    @Test
    @DisplayName("测试队列满载触发 MonitoredCallerRunsPolicy 降级反压与拒绝计数统计")
    void testCallerRunsBackpressureAndRejectionCount() throws InterruptedException {
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

    @Test
    @DisplayName("测试线程池关闭后触发 RejectedExecutionException 且不累计降级计数")
    void testShutdownRejectionBehavior() {
        ThreadPoolTaskExecutor smallExecutor = new ThreadPoolTaskExecutor();
        MonitoredCallerRunsPolicy policy = new MonitoredCallerRunsPolicy();
        smallExecutor.setCorePoolSize(1);
        smallExecutor.setMaxPoolSize(1);
        smallExecutor.setQueueCapacity(2);
        smallExecutor.setThreadNamePrefix("test-shutdown-");
        smallExecutor.setRejectedExecutionHandler(policy);
        smallExecutor.initialize();

        ThreadPoolExecutor tpe = smallExecutor.getThreadPoolExecutor();
        assertNotNull(tpe);
        smallExecutor.destroy();

        assertTrue(tpe.isShutdown(), "线程池应处于已关闭状态");

        // 1. 显式调用 policy.rejectedExecution 必须抛出 RejectedExecutionException
        RejectedExecutionException ex = assertThrows(
                RejectedExecutionException.class,
                () -> policy.rejectedExecution(() -> {}, tpe),
                "已关闭的线程池调用拒绝策略必须抛出 RejectedExecutionException"
        );
        assertTrue(ex.getMessage().contains("already shutdown"),
                "异常信息应包含 already shutdown，当前为: " + ex.getMessage());

        // 2. 直接向已关闭底层 ThreadPoolExecutor 提交任务，验证真实执行链路抛出 RejectedExecutionException
        assertThrows(
                RejectedExecutionException.class,
                () -> tpe.execute(() -> {}),
                "向已关闭的底层 ThreadPoolExecutor 提交任务应触发 RejectedExecutionException"
        );

        // 3. 通过 Spring ThreadPoolTaskExecutor 包装器提交任务，验证抛出 Spring TaskRejectedException
        assertThrows(
                TaskRejectedException.class,
                () -> smallExecutor.execute(() -> {}),
                "向已关闭的 ThreadPoolTaskExecutor 提交任务应抛出 TaskRejectedException"
        );

        // 4. 防御性检查：executor 为 null 时安全抛出异常，不产生 NPE
        assertThrows(
                RejectedExecutionException.class,
                () -> policy.rejectedExecution(() -> {}, null),
                "executor 为 null 时应安全抛出 RejectedExecutionException"
        );

        assertEquals(0, policy.getRejectCount(), "关闭期所有拒绝行为均不应计入 CallerRuns 降级反压指标");
    }

    @Test
    @DisplayName("测试 P1-1.5 架构约束：不实现 AsyncConfigurer、移除冗余 EnableAsync 与 Primary")
    void testAsyncArchitecturalConstraints() throws NoSuchMethodException {
        // 1. PersistenceAsyncConfig 绝不实现 AsyncConfigurer
        assertFalse(AsyncConfigurer.class.isAssignableFrom(PersistenceAsyncConfig.class),
                "PersistenceAsyncConfig 严禁实现 AsyncConfigurer，避免全局隐式接管默认异步调度");

        // 2. PersistenceAsyncConfig 移除 @EnableAsync
        assertFalse(PersistenceAsyncConfig.class.isAnnotationPresent(EnableAsync.class),
                "PersistenceAsyncConfig 不应重复声明 @EnableAsync");

        // 3. EdgeCoreApplication 统一保留 @EnableAsync
        assertTrue(EdgeCoreApplication.class.isAnnotationPresent(EnableAsync.class),
                "EdgeCoreApplication 必须作为唯一顶层声明 @EnableAsync 的入口");

        // 4. persistenceExecutor Bean 不应带有 @Primary 注解
        Method executorMethod = PersistenceAsyncConfig.class.getMethod("persistenceExecutor", MonitoredCallerRunsPolicy.class);
        assertFalse(executorMethod.isAnnotationPresent(Primary.class),
                "persistenceExecutor 严禁标记为 @Primary，防止无指定 executor 的 @Async 误占配额");
    }

    @Test
    @DisplayName("测试 EdgeProperties 为空或缺省时的防御性配置装配")
    void testDefensiveConfigurationWithNullProperties() {
        // null properties
        PersistenceAsyncConfig configWithNull = new PersistenceAsyncConfig(null);
        MonitoredCallerRunsPolicy policy = configWithNull.persistenceRejectionPolicy();
        ThreadPoolTaskExecutor executor = configWithNull.persistenceExecutor(policy);

        try {
            assertEquals(2, executor.getCorePoolSize(), "缺省核心线程数必须为 2");
            assertEquals(4, executor.getMaxPoolSize(), "缺省最大线程数必须为 4");
            assertEquals(500, executor.getQueueCapacity(), "缺省队列容量必须为 500");
        } finally {
            executor.destroy();
        }
    }
}
