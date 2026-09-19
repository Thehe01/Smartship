package com.smartship.edge.benchmark;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.pool.MonitoredCallerRunsPolicy;
import com.smartship.edge.routing.pool.PersistenceAsyncConfig;
import com.smartship.edge.routing.pool.PersistencePoolMetrics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P1-4 Persistence Executor 反压 Benchmark（integration benchmark，非 JMH）。
 * <p>
 * 使用生产配额（core=2 / max=4 / queue=500 / CallerRuns），以可控阻塞任务模拟
 * 不同 DB 耗时，测量提交延迟、队列峰值、降级计数与有效吞吐。
 * 此处的 {@code reject} 语义是 {@code ThreadPoolExecutor reject → CallerRuns fallback
 * （提交线程同步执行）}，不是数据丢弃：断言全部任务最终完成。
 */
@Tag("benchmark")
@DisplayName("P1-4 Benchmark: persistence backpressure")
class PersistenceBackpressureBenchmarkTest {

    private static final BenchmarkRecorder RECORDER = new BenchmarkRecorder(params());

    private static Map<String, String> params() {
        return Map.of(
                "benchmark.persistence.tasks", prop("benchmark.persistence.tasks", "200,800"),
                "benchmark.persistence.latencyMs", prop("benchmark.persistence.latencyMs", "5,25"),
                "pool.core", "2", "pool.max", "4", "pool.queue", "500");
    }

    private static String prop(String key, String def) {
        return System.getProperty(key, def);
    }

    @AfterAll
    static void flushReport() {
        RECORDER.flush();
    }

    private static List<Integer> intList(String key, String def) {
        List<Integer> out = new ArrayList<>();
        for (String part : prop(key, def).split(",")) {
            out.add(Integer.parseInt(part.trim()));
        }
        return out;
    }

    @Test
    @DisplayName("caller-runs-deterministic: 队列填满后确定性触发 CallerRuns（非计时）")
    void callerRunsDeterministic() throws Exception {
        EdgeProperties properties = new EdgeProperties();
        PersistenceAsyncConfig config = new PersistenceAsyncConfig(properties);
        MonitoredCallerRunsPolicy policy = config.persistenceRejectionPolicy();
        ThreadPoolTaskExecutor executor = config.persistenceExecutor(policy);
        try {
            assertEquals(2, executor.getCorePoolSize());
            assertEquals(4, executor.getMaxPoolSize());
            assertEquals(500, executor.getQueueCapacity());

            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch coreOccupied = new CountDownLatch(2);
            CountDownLatch scaledOccupied = new CountDownLatch(2);
            Runnable blocker = () -> {
                try {
                    release.await(15, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
            // 1. 先占满 2 个核心线程（core=2 时前两个任务直接上线程，不经过队列）
            for (int i = 0; i < 2; i++) {
                executor.submit(() -> {
                    coreOccupied.countDown();
                    blocker.run();
                });
            }
            assertTrue(coreOccupied.await(5, TimeUnit.SECONDS));
            // 2. 填满 500 队列（队列未满前不向 max 扩容，此时仍是 2 线程）
            CountDownLatch queued = new CountDownLatch(500);
            for (int i = 0; i < 500; i++) {
                executor.submit(() -> {
                    try {
                        release.await(15, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        queued.countDown();
                    }
                });
            }
            Thread.sleep(300); // 让排队任务稳定入列
            assertEquals(500, executor.getThreadPoolExecutor().getQueue().size());
            // 3. 再提交 2 个：队列已满，触发向 max=4 扩容并由新线程执行
            for (int i = 0; i < 2; i++) {
                executor.submit(() -> {
                    scaledOccupied.countDown();
                    blocker.run();
                });
            }
            assertTrue(scaledOccupied.await(5, TimeUnit.SECONDS), "必须扩容到 4 线程");
            assertEquals(4, executor.getThreadPoolExecutor().getPoolSize());

            // 4. 再提交 1 个：4 线程全占 + 500 队列全满，必走 CallerRuns
            String caller = Thread.currentThread().getName();
            AtomicBoolean ranOnCaller = new AtomicBoolean(false);
            executor.submit(() -> {
                if (Thread.currentThread().getName().equals(caller)) {
                    ranOnCaller.set(true);
                }
            });
            assertTrue(ranOnCaller.get(), "溢出任务必须在提交线程同步执行");
            assertEquals(1, policy.getRejectCount());

            release.countDown();
            assertTrue(queued.await(15, TimeUnit.SECONDS), "全部排队任务必须最终执行");
            BenchmarkRecorder.Scenario s = RECORDER.scenario("persistence-caller-runs-proof")
                    .param("mode", "deterministic")
                    .note("reject_count=1 为 CallerRuns fallback 次数；505 个任务全部执行，无丢弃");
            s.count("submitted", 505).count("completed", 505).count("reject_count", 1);
            RECORDER.complete(s);
        } finally {
            executor.destroy();
        }
    }

    @Test
    @DisplayName("backpressure-matrix: 不同任务量 × DB 耗时下的提交延迟与吞吐")
    void backpressureMatrix() throws Exception {
        for (int tasks : intList("benchmark.persistence.tasks", "200,800")) {
            for (int latencyMs : intList("benchmark.persistence.latencyMs", "5,25")) {
                runCombo(tasks, latencyMs);
            }
        }
    }

    private void runCombo(int tasks, int latencyMs) throws Exception {
        EdgeProperties properties = new EdgeProperties();
        PersistenceAsyncConfig config = new PersistenceAsyncConfig(properties);
        MonitoredCallerRunsPolicy policy = config.persistenceRejectionPolicy();
        ThreadPoolTaskExecutor executor = config.persistenceExecutor(policy);
        PersistencePoolMetrics metrics = new PersistencePoolMetrics(executor, policy);
        try {
            BenchmarkRecorder.Scenario s = RECORDER.scenario(
                    "persistence-backpressure-t" + tasks + "-l" + latencyMs)
                    .param("tasks", tasks)
                    .param("db_latency_ms", latencyMs)
                    .param("warmup_runs", 1)
                    .param("measured_runs", 3)
                    .note("submit latency 为单次 submit() 调用耗时（含 CallerRuns 内联执行）；"
                            + "reject_count 为降级次数；CallerRuns 内联任务不经过 worker 线程的 afterExecute，"
                            + "因此 completedTaskCount 天然少计 reject 部分，完成判定以 completed + reject 为准；"
                            + "所有任务均执行，无丢弃；计数器取末轮 measured 单轮值");
            for (int run = 0; run < 4; run++) {
                boolean warmup = run == 0;
                boolean last = run == 3;
                policy.resetRejectCount();
                long completedBase = metrics.getCompletedTaskCount();
                CountDownLatch latch = new CountDownLatch(tasks);
                List<Long> submitNanos = new ArrayList<>(tasks);
                long[] queuePeak = {0};
                long t0 = System.nanoTime();
                for (int i = 0; i < tasks; i++) {
                    long m0 = System.nanoTime();
                    executor.submit(() -> {
                        try {
                            Thread.sleep(latencyMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            latch.countDown();
                        }
                    });
                    submitNanos.add(System.nanoTime() - m0);
                    long q = executor.getThreadPoolExecutor().getQueue().size();
                    if (q > queuePeak[0]) {
                        queuePeak[0] = q;
                    }
                }
                assertTrue(latch.await(120, TimeUnit.SECONDS), "任务必须全部执行完毕");
                // 收敛等待：worker 线程完成的任务经 afterExecute 计入 completedTaskCount
                // （略晚于任务 finally），CallerRuns 内联任务直接计入 reject 位
                long deadline = System.currentTimeMillis() + 10_000;
                while (metrics.getCompletedTaskCount() - completedBase + policy.getRejectCount() < tasks
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(20);
                }
                double elapsedSec = (System.nanoTime() - t0) / 1e9;
                if (!warmup) {
                    for (long nanos : submitNanos) {
                        s.recordLatency("submit", nanos);
                    }
                    long completedDelta = metrics.getCompletedTaskCount() - completedBase;
                    long accounted = completedDelta + policy.getRejectCount();
                    assertEquals(tasks, accounted,
                            "t=" + tasks + "/l=" + latencyMs + ": worker 完成 + CallerRuns 必须等于提交数，无丢弃");
                    assertTrue(queuePeak[0] <= 500,
                            "t=" + tasks + "/l=" + latencyMs + ": 队列必须有界 (<=500)");
                    if (last) {
                        s.count("submitted", tasks)
                                .count("completed_worker", completedDelta)
                                .count("reject_count", policy.getRejectCount())
                                .observePeak("queue_peak", queuePeak[0]);
                    }
                    s.addRunThroughput(tasks / elapsedSec);
                }
            }
            RECORDER.complete(s);
        } finally {
            executor.destroy();
        }
    }
}
