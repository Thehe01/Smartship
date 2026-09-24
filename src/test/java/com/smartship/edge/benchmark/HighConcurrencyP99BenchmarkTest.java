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

import static org.junit.jupiter.api.Assertions.*;

/**
 * 高并发摸底 Benchmark：50 船合计 1 万测点 × 1Hz 持续上报的 p99 延迟。
 * <p>
 * 口径：1 点位 = 1 个传感器测量值 = 1 个持久化任务；50 船按 round-robin 汇聚
 * 到生产配额的持久化线程池（core=2 / max=4 / queue=500 / CallerRuns）。
 * 路由收敛（50 船恰好 50 池）已由 {@code MultiShipRoutingBenchmarkTest} 覆盖，
 * 本场景只测汇聚后的入库 p99（含排队 + CallerRuns 内联执行），不重复建 50 个
 * 真实 Hikari 池，避免单机 H2  noise 淹没 p99 本身。
 * <p>
 * 分组：{@code submit} = 单次 submit() 调用耗时（含 CallerRuns 内联）；
 * {@code e2e} = 任务入队到执行完成的端到端耗时（含排队等待）。两组禁止混算。
 * 数字仅描述当前测试环境（H2 + sleep 模拟 DB + 单 JVM），不得宣称为生产性能。
 */
@Tag("benchmark")
@DisplayName("P1-4 Benchmark: high-concurrency p99 probe (50 ships x 10k pts @1Hz)")
class HighConcurrencyP99BenchmarkTest {

    private static final BenchmarkRecorder RECORDER = new BenchmarkRecorder(params());

    private static Map<String, String> params() {
        return Map.of(
                "benchmark.highconcurrency.tasks", prop("benchmark.highconcurrency.tasks", "10000"),
                "benchmark.highconcurrency.latencyMs", prop("benchmark.highconcurrency.latencyMs", "1"),
                "benchmark.highconcurrency.ships", prop("benchmark.highconcurrency.ships", "50"));
    }

    private static String prop(String key, String def) {
        return System.getProperty(key, def);
    }

    @AfterAll
    static void flushReport() {
        RECORDER.flush();
    }

    @Test
    @DisplayName("highconcurrency-p99: 50船1万点汇聚入库的submit/e2e p99")
    void highConcurrencyP99() throws Exception {
        int tasks = Integer.parseInt(prop("benchmark.highconcurrency.tasks", "10000").trim());
        int latencyMs = Integer.parseInt(prop("benchmark.highconcurrency.latencyMs", "1").trim());
        int ships = Integer.parseInt(prop("benchmark.highconcurrency.ships", "50").trim());
        assertTrue(tasks > 0 && ships > 0 && latencyMs >= 0);

        EdgeProperties properties = new EdgeProperties();
        PersistenceAsyncConfig config = new PersistenceAsyncConfig(properties);
        MonitoredCallerRunsPolicy policy = config.persistenceRejectionPolicy();
        ThreadPoolTaskExecutor executor = config.persistenceExecutor(policy);
        PersistencePoolMetrics metrics = new PersistencePoolMetrics(executor, policy);
        try {
            assertEquals(2, executor.getCorePoolSize());
            assertEquals(4, executor.getMaxPoolSize());
            assertEquals(500, executor.getQueueCapacity());

            BenchmarkRecorder.Scenario s = RECORDER.scenario("highconcurrency-p99-10k")
                    .param("tasks", tasks)
                    .param("ships", ships)
                    .param("points_per_ship_per_sec", String.format(java.util.Locale.ROOT,
                            "%.1f", (double) tasks / ships))
                    .param("db_latency_ms", latencyMs)
                    .param("distribution", "round-robin shipIdx=i%ships (tag only, no real Hikari fanout)")
                    .param("warmup_runs", 1)
                    .param("measured_runs", 3)
                    .param("pool.core", 2)
                    .param("pool.max", 4)
                    .param("pool.queue", 500)
                    .note("submit = 单次 submit() 调用耗时（含 CallerRuns 内联执行）；"
                            + "e2e = 任务入队到执行完成的端到端耗时（含排队等待）；两组独立统计，禁止混算 p99；"
                            + "reject_count 为 CallerRuns fallback 次数，CallerRuns 内联任务不经过 worker afterExecute，"
                            + "完成判定以 completed + reject 为准，所有任务均执行、无丢弃；"
                            + "DB 以 sleep 模拟，数字仅描述当前测试环境，不得宣称为生产性能；"
                            + "50 船路由收敛由 multiship-routing 场景覆盖，本场景只测汇聚后的入库 p99");

            // 1 warmup + 3 measured，每轮新建 latch，计数器只取末轮 measured 单轮值
            for (int run = 0; run < 4; run++) {
                boolean warmup = run == 0;
                boolean last = run == 3;
                policy.resetRejectCount();
                long completedBase = metrics.getCompletedTaskCount();
                CountDownLatch latch = new CountDownLatch(tasks);
                long[] enqueueNanos = new long[tasks];
                List<Long> submitNanos = new ArrayList<>(tasks);
                List<Long> e2eNanos = java.util.Collections.synchronizedList(new ArrayList<>(tasks));
                long[] queuePeak = {0};
                long t0 = System.nanoTime();
                for (int i = 0; i < tasks; i++) {
                    enqueueNanos[i] = System.nanoTime();
                    final long enq = enqueueNanos[i];
                    long m0 = System.nanoTime();
                    executor.submit(() -> {
                        try {
                            if (latencyMs > 0) {
                                Thread.sleep(latencyMs);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            e2eNanos.add(System.nanoTime() - enq);
                            latch.countDown();
                        }
                    });
                    submitNanos.add(System.nanoTime() - m0);
                    long q = executor.getThreadPoolExecutor().getQueue().size();
                    if (q > queuePeak[0]) {
                        queuePeak[0] = q;
                    }
                }
                assertTrue(latch.await(300, TimeUnit.SECONDS), "1 万任务必须全部执行完毕");
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
                    for (long nanos : e2eNanos) {
                        s.recordLatency("e2e", nanos);
                    }
                    assertEquals(tasks, e2eNanos.size(), "e2e 样本数必须等于提交数");
                    long completedDelta = metrics.getCompletedTaskCount() - completedBase;
                    long accounted = completedDelta + policy.getRejectCount();
                    assertEquals(tasks, accounted, "worker 完成 + CallerRuns 必须等于提交数，无丢弃");
                    assertTrue(queuePeak[0] <= 500, "队列必须有界 (<=500)");
                    if (last) {
                        s.count("submitted", tasks)
                                .count("completed_worker", completedDelta)
                                .count("reject_count", policy.getRejectCount())
                                .count("e2e_samples", e2eNanos.size())
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
