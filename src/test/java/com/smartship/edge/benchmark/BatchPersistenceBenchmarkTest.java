package com.smartship.edge.benchmark;

import com.smartship.edge.benchmark.support.BenchmarkFixtures;
import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PersistenceThrottle;
import com.smartship.edge.routing.pool.MonitoredCallerRunsPolicy;
import com.smartship.edge.routing.pool.PersistenceAsyncConfig;
import com.smartship.edge.routing.pool.PersistencePoolMetrics;
import com.smartship.edge.routing.service.EnginePoint;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import com.smartship.edge.routing.service.WriteBatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;

/**
 * 批量写入突破对照 Benchmark（integration benchmark，非 JMH，真实 H2 写入）。
 * <p>
 * 同 1 万行规模下对照两种走法：
 * A {@code batch-single-row}：1 行 = 1 池任务 = 1 次 JDBC round-trip（现状成本模型）；
 * B {@code batch-batched}：按船攒批（默认 500 行/批），1 批 = 1 池任务 = 每船 1 次 batchUpdate。
 * <p>
 * 诚实声明：单行臂直接提交任务（无 Spring @Async 那一跳），数字相对生产略乐观，
 * 因此批量臂的优势是被低估而非夸大；50 船只做 mmsi 标签轮转 + 服务内按船分组，
 * 路由收敛本身由 multiship 场景覆盖；H2 单库串行执行分组 batchUpdate，比生产
 * 50 个分船库并行更慢，同样是保守口径。
 */
@Tag("benchmark")
@DisplayName("P1-4 Benchmark: batch write breakthrough (real H2)")
class BatchPersistenceBenchmarkTest {

    private static final BenchmarkRecorder RECORDER = new BenchmarkRecorder(params());

    private static Map<String, String> params() {
        return Map.of(
                "benchmark.batch.rows", prop("benchmark.batch.rows", "10000"),
                "benchmark.batch.size", prop("benchmark.batch.size", "500"),
                "benchmark.batch.ships", prop("benchmark.batch.ships", "50"));
    }

    private static String prop(String key, String def) {
        return System.getProperty(key, def);
    }

    @AfterAll
    static void flushReport() {
        RECORDER.flush();
    }

    private static String mmsiOf(int i) {
        return "413" + String.format("%06d", i);
    }

    /** 单轮夹具：全新 H2 船库 + 真实持久化服务（无 Spring @Async，调用即同步执行）。 */
    private static final class Fixture implements AutoCloseable {
        final com.zaxxer.hikari.HikariDataSource ds;
        final JdbcTemplate jt;
        final NmeaDataPersistenceService persist;

        Fixture() {
            // 生产级连接池（同分船库配额 max=5）：DriverManagerDataSource 每次新建物理连接，
            // 在万级突发下会放大 H2 MVStore 自增键竞争，属测试夹具失真而非生产行为。
            String dbName = "batch_bench_" + System.nanoTime();
            ds = new com.zaxxer.hikari.HikariDataSource();
            ds.setJdbcUrl("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1;MODE=MySQL");
            ds.setUsername("sa");
            ds.setPassword("");
            ds.setMaximumPoolSize(5);
            ds.setPoolName("bench-pool-" + dbName);
            jt = new JdbcTemplate(ds);
            BenchmarkFixtures.createShipTables(jt);
            EdgeProperties properties = new EdgeProperties();
            properties.setMmsi("batch-bench");
            properties.setSchemaReady(true);
            properties.getCollect().getPersist().setEnabled(true);
            properties.getCollect().getPersist().setMinWriteIntervalSeconds(0);
            persist = new NmeaDataPersistenceService(
                    jt, properties, new PersistenceThrottle(properties));
        }

        long engineRows() {
            return BenchmarkFixtures.count(jt, "zncb_engine_data");
        }

        @Override
        public void close() {
            ds.close();
        }
    }

    private static ThreadPoolTaskExecutor newExecutor(MonitoredCallerRunsPolicy[] policyOut,
                                                      PersistencePoolMetrics[] metricsOut) {
        EdgeProperties properties = new EdgeProperties();
        PersistenceAsyncConfig config = new PersistenceAsyncConfig(properties);
        MonitoredCallerRunsPolicy policy = config.persistenceRejectionPolicy();
        ThreadPoolTaskExecutor executor = config.persistenceExecutor(policy);
        policyOut[0] = policy;
        metricsOut[0] = new PersistencePoolMetrics(executor, policy);
        return executor;
    }

    @Test
    @DisplayName("batch-single-row: 现状 1行1任务1round-trip 的 1万行基线")
    void singleRowBaseline() throws Exception {
        int rows = Integer.parseInt(prop("benchmark.batch.rows", "10000").trim());
        int ships = Integer.parseInt(prop("benchmark.batch.ships", "50").trim());
        BenchmarkRecorder.Scenario s = RECORDER.scenario("batch-single-row-10k")
                .param("rows", rows)
                .param("ships", ships)
                .param("db", "real-H2-single-insert-per-task")
                .param("warmup_runs", 1)
                .param("measured_runs", 3)
                .param("pool.core", 2)
                .param("pool.max", 4)
                .param("pool.queue", 500)
                .note("现状成本模型：1 行 = 1 池任务 = 1 次 round-trip（经 saveEngineBatch(List.of) 以精确计数）；"
                        + "无 Spring @Async 一跳，数字相对生产略乐观，批量优势因此被低估；"
                        + "H2 MVStore 自增键突发并发偶发重复键（MySQL 无此行为），由生产方法的事务回滚+逐行补写兜底，耗时计入 e2e；"
                        + "e2e = 入队到执行完成；完成判定 completed + reject；计数器取末轮 measured 单轮值");
        for (int run = 0; run < 4; run++) {
            boolean warmup = run == 0;
            boolean last = run == 3;
            try (Fixture f = new Fixture()) {
                MonitoredCallerRunsPolicy[] policyOut = new MonitoredCallerRunsPolicy[1];
                PersistencePoolMetrics[] metricsOut = new PersistencePoolMetrics[1];
                ThreadPoolTaskExecutor executor = newExecutor(policyOut, metricsOut);
                try {
                    MonitoredCallerRunsPolicy policy = policyOut[0];
                    PersistencePoolMetrics metrics = metricsOut[0];
                    policy.resetRejectCount();
                    long completedBase = metrics.getCompletedTaskCount();
                    CountDownLatch latch = new CountDownLatch(rows);
                    List<Long> submitNanos = new ArrayList<>(rows);
                    List<Long> e2eNanos = java.util.Collections.synchronizedList(new ArrayList<>(rows));
                    long[] queuePeak = {0};
                    long t0 = System.nanoTime();
                    for (int i = 0; i < rows; i++) {
                        final String mmsi = mmsiOf(i % ships);
                        final int slave = 1 + (i % 2);
                        // 与生产单行路径相同的成本模型（1 任务 + 1 次 round-trip），
                        // 经 saveEngineBatch(List.of) 以便精确统计落库行数；
                        // saveEngineBatch 内部显式事务 + 回滚 + 逐行补写，保证计数精确；
                        // H2 MVStore 自增键突发并发偶发重复键（MySQL 无此行为），补写耗时计入 e2e。
                        final EnginePoint point = EnginePoint.now(mmsi, slave,
                                1500.0, 85.0, 0.5, 0.4, 420.0, 0.25, 2.8, 75.0,
                                24.5, 12000, 1, 0, 0);
                        long enq = System.nanoTime();
                        long m0 = System.nanoTime();
                        executor.submit(() -> {
                            try {
                                int written = f.persist.saveEngineBatch(List.of(point));
                                assertEquals(1, written, "单行落库必须成功");
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
                    assertTrue(latch.await(300, TimeUnit.SECONDS), "全部行任务必须执行完毕");
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (metrics.getCompletedTaskCount() - completedBase + policy.getRejectCount() < rows
                            && System.currentTimeMillis() < deadline) {
                        Thread.sleep(20);
                    }
                    double elapsedSec = (System.nanoTime() - t0) / 1e9;
                    assertEquals(rows, f.engineRows(), "H2 必须落下全部行");
                    if (!warmup) {
                        for (long nanos : submitNanos) {
                            s.recordLatency("submit", nanos);
                        }
                        for (long nanos : e2eNanos) {
                            s.recordLatency("e2e", nanos);
                        }
                        long completedDelta = metrics.getCompletedTaskCount() - completedBase;
                        assertEquals(rows, completedDelta + policy.getRejectCount(), "无丢弃");
                        assertTrue(queuePeak[0] <= 500, "队列有界");
                        if (last) {
                            s.count("rows", rows)
                                    .count("engine_rows", f.engineRows())
                                    .count("completed_worker", completedDelta)
                                    .count("reject_count", policy.getRejectCount())
                                    .observePeak("queue_peak", queuePeak[0]);
                        }
                        s.addRunThroughput(rows / elapsedSec);
                    }
                } finally {
                    executor.destroy();
                }
            }
        }
        RECORDER.complete(s);
    }

    @Test
    @DisplayName("batch-batched: 攒批 1批1任务每船1batchUpdate 的 1万行对照")
    void batchedBreakthrough() throws Exception {
        int rows = Integer.parseInt(prop("benchmark.batch.rows", "10000").trim());
        int ships = Integer.parseInt(prop("benchmark.batch.ships", "50").trim());
        int batchSize = Integer.parseInt(prop("benchmark.batch.size", "500").trim());
        BenchmarkRecorder.Scenario s = RECORDER.scenario("batch-batched-10k")
                .param("rows", rows)
                .param("ships", ships)
                .param("batch_size", batchSize)
                .param("db", "real-H2-batchUpdate-per-ship-per-batch")
                .param("warmup_runs", 1)
                .param("measured_runs", 3)
                .param("pool.core", 2)
                .param("pool.max", 4)
                .param("pool.queue", 500)
                .note("突破走法：WriteBatcher 按船攒满 " + batchSize + " 行即作为 1 个池任务提交，"
                        + "任务内按船分组 batchUpdate；尾批 drainAll 显式刷出；"
                        + "同单行臂对比 e2e p99 与吞吐；计数器取末轮 measured 单轮值");
        for (int run = 0; run < 4; run++) {
            boolean warmup = run == 0;
            boolean last = run == 3;
            try (Fixture f = new Fixture()) {
                MonitoredCallerRunsPolicy[] policyOut = new MonitoredCallerRunsPolicy[1];
                PersistencePoolMetrics[] metricsOut = new PersistencePoolMetrics[1];
                ThreadPoolTaskExecutor executor = newExecutor(policyOut, metricsOut);
                try {
                    MonitoredCallerRunsPolicy policy = policyOut[0];
                    PersistencePoolMetrics metrics = metricsOut[0];
                    policy.resetRejectCount();
                    long completedBase = metrics.getCompletedTaskCount();
                    List<EnginePoint> all = new ArrayList<>(rows);
                    for (int i = 0; i < rows; i++) {
                        all.add(EnginePoint.now(mmsiOf(i % ships), 1 + (i % 2),
                                1500.0, 85.0, 0.5, 0.4, 420.0, 0.25, 2.8, 75.0,
                                24.5, 12000, 1, 0, 0));
                    }
                    long[] enq = new long[rows];
                    CountDownLatch latch = new CountDownLatch(rows);
                    List<Long> submitNanos = java.util.Collections.synchronizedList(new ArrayList<>());
                    List<Long> e2eNanos = java.util.Collections.synchronizedList(new ArrayList<>(rows));
                    long[] queuePeak = {0};
                    long[] batchTasks = {0};
                    WriteBatcher<String, Integer> batcher = new WriteBatcher<>(batchSize);
                    long t0 = System.nanoTime();
                    java.util.function.Consumer<List<Integer>> flush = batch -> {
                        List<EnginePoint> sub = new ArrayList<>(batch.size());
                        for (int idx : batch) {
                            sub.add(all.get(idx));
                        }
                        long m0 = System.nanoTime();
                        executor.submit(() -> {
                            try {
                                int written = f.persist.saveEngineBatch(sub);
                                assertEquals(sub.size(), written, "整批必须全部落库");
                            } finally {
                                long done = System.nanoTime();
                                for (int idx : batch) {
                                    e2eNanos.add(done - enq[idx]);
                                    latch.countDown();
                                }
                            }
                        });
                        submitNanos.add(System.nanoTime() - m0);
                        batchTasks[0]++;
                        long q = executor.getThreadPoolExecutor().getQueue().size();
                        if (q > queuePeak[0]) {
                            queuePeak[0] = q;
                        }
                    };
                    for (int i = 0; i < rows; i++) {
                        enq[i] = System.nanoTime();
                        batcher.add(mmsiOf(i % ships), i, flush);
                    }
                    batcher.drainAll(flush);
                    assertEquals(0, batcher.bufferedRows(), "尾批必须刷空");
                    assertTrue(latch.await(300, TimeUnit.SECONDS), "全部行必须落库");
                    long deadline = System.currentTimeMillis() + 10_000;
                    long expectedTasks = batchTasks[0];
                    while (metrics.getCompletedTaskCount() - completedBase + policy.getRejectCount() < expectedTasks
                            && System.currentTimeMillis() < deadline) {
                        Thread.sleep(20);
                    }
                    double elapsedSec = (System.nanoTime() - t0) / 1e9;
                    assertEquals(rows, f.engineRows(), "H2 必须落下全部行");
                    if (!warmup) {
                        for (long nanos : submitNanos) {
                            s.recordLatency("submit_batch", nanos);
                        }
                        for (long nanos : e2eNanos) {
                            s.recordLatency("e2e", nanos);
                        }
                        long completedDelta = metrics.getCompletedTaskCount() - completedBase;
                        assertEquals(expectedTasks, completedDelta + policy.getRejectCount(),
                                "批量任务无丢弃");
                        assertTrue(queuePeak[0] <= 500, "队列有界");
                        if (last) {
                            s.count("rows", rows)
                                    .count("engine_rows", f.engineRows())
                                    .count("batch_tasks", expectedTasks)
                                    .count("completed_worker", completedDelta)
                                    .count("reject_count", policy.getRejectCount())
                                    .observePeak("queue_peak", queuePeak[0]);
                        }
                        s.addRunThroughput(rows / elapsedSec);
                    }
                } finally {
                    executor.destroy();
                }
            }
        }
        RECORDER.complete(s);
    }
}
