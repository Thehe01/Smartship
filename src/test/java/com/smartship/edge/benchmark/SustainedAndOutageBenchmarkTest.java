package com.smartship.edge.benchmark;

import com.smartship.edge.benchmark.simulator.CountingMqttGateway;
import com.smartship.edge.benchmark.support.BenchmarkFixtures;
import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PersistenceThrottle;
import com.smartship.edge.routing.pool.MonitoredCallerRunsPolicy;
import com.smartship.edge.routing.pool.PersistenceAsyncConfig;
import com.smartship.edge.routing.pool.PersistencePoolMetrics;
import com.smartship.edge.routing.service.EnginePoint;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import com.smartship.edge.routing.service.WriteBatcher;
import com.smartship.edge.uploader.DatabaseUploadPoller;
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
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;

/**
 * 持续负载与长断网 Benchmark（integration benchmark，非 JMH）。
 * <p>
 * 场景一 {@code sustained-2500-paced}：50 船合计 2500 点/秒起搏到达（4 秒/轮），
 * 经批量路径（WriteBatcher + saveEngineBatch）真实 H2 落库，测 submit/e2e p99。
 * 起搏到达下 e2e 含攒批等待——这是批量换吞吐的诚实代价，调小 batch 即换延迟。
 * <p>
 * 场景二 {@code outage-5min-backfill}：2500 点/秒 × 300 秒 = 75 万行积压，
 * 中途断网冻结游标，恢复后补传：断言游标精确、积压归零、逐行覆盖（零丢失）、
 * 切断行重传 msg_id 恒定、总 attempts 精确可算。75 万行只跑单 measured 轮
 * （另用 5000 行小轮 warmup），属语义型大积压场景的必要偏离，如实声明。
 */
@Tag("benchmark")
@DisplayName("P1-4 Benchmark: sustained 2500/s p99 + 5min outage backfill")
class SustainedAndOutageBenchmarkTest {

    private static final String SHIP_ID = "ship-bench";
    private static final String MMSI = "413999999";
    private static final String TABLE = "zncb_gps_data";
    private static final BenchmarkRecorder RECORDER = new BenchmarkRecorder(params());

    private static Map<String, String> params() {
        return Map.of(
                "benchmark.sustained.rows", prop("benchmark.sustained.rows", "10000"),
                "benchmark.sustained.ratePerSec", prop("benchmark.sustained.ratePerSec", "2500"),
                "benchmark.sustained.ships", prop("benchmark.sustained.ships", "50"),
                "benchmark.sustained.batch", prop("benchmark.sustained.batch", "100"),
                "benchmark.outage.rows", prop("benchmark.outage.rows", "750000"),
                "benchmark.outage.pollBatch", prop("benchmark.outage.pollBatch", "10000"),
                "benchmark.outage.failAfter", prop("benchmark.outage.failAfter", "100000"));
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

    // ==================== 场景一：起搏 2500/s ====================

    private static final class PersistFixture implements AutoCloseable {
        final com.zaxxer.hikari.HikariDataSource ds;
        final JdbcTemplate jt;
        final NmeaDataPersistenceService persist;

        PersistFixture() {
            String dbName = "sustained_bench_" + System.nanoTime();
            ds = new com.zaxxer.hikari.HikariDataSource();
            ds.setJdbcUrl("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1;MODE=MySQL");
            ds.setUsername("sa");
            ds.setPassword("");
            ds.setMaximumPoolSize(5);
            ds.setPoolName("bench-pool-" + dbName);
            jt = new JdbcTemplate(ds);
            BenchmarkFixtures.createShipTables(jt);
            EdgeProperties properties = new EdgeProperties();
            properties.setMmsi("sustained-bench");
            properties.setSchemaReady(true);
            properties.getCollect().getPersist().setEnabled(true);
            properties.getCollect().getPersist().setMinWriteIntervalSeconds(0);
            persist = new NmeaDataPersistenceService(
                    jt, properties, new PersistenceThrottle(properties));
        }

        @Override
        public void close() {
            ds.close();
        }
    }

    @Test
    @DisplayName("sustained-2500-paced: 50船2500点/秒起搏到达的submit/e2e p99")
    void sustained2500Paced() throws Exception {
        int rows = Integer.parseInt(prop("benchmark.sustained.rows", "10000").trim());
        int rate = Integer.parseInt(prop("benchmark.sustained.ratePerSec", "2500").trim());
        int ships = Integer.parseInt(prop("benchmark.sustained.ships", "50").trim());
        int batchSize = Integer.parseInt(prop("benchmark.sustained.batch", "100").trim());
        long intervalNanos = 1_000_000_000L / rate;
        BenchmarkRecorder.Scenario s = RECORDER.scenario("sustained-2500-paced")
                .param("rows", rows)
                .param("rate_per_sec", rate)
                .param("ships", ships)
                .param("batch_size", batchSize)
                .param("arrival", "paced LockSupport.parkNanos")
                .param("db", "real-H2-batchUpdate-per-ship-per-batch")
                .param("warmup_runs", 1)
                .param("measured_runs", 3)
                .param("pool.core", 2)
                .param("pool.max", 4)
                .param("pool.queue", 500)
                .note("到达按 1/rate 起搏（非突发），e2e = 入队到落库完成，含攒批等待；"
                        + "攒批等待是批量换吞吐的代价，调小 batch 即换延迟；"
                        + "计数器取末轮 measured 单轮值");
        for (int run = 0; run < 4; run++) {
            boolean warmup = run == 0;
            boolean last = run == 3;
            try (PersistFixture f = new PersistFixture()) {
                EdgeProperties properties = new EdgeProperties();
                PersistenceAsyncConfig config = new PersistenceAsyncConfig(properties);
                MonitoredCallerRunsPolicy policy = config.persistenceRejectionPolicy();
                ThreadPoolTaskExecutor executor = config.persistenceExecutor(policy);
                PersistencePoolMetrics metrics = new PersistencePoolMetrics(executor, policy);
                try {
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
                    long t0 = System.nanoTime();
                    long next = t0;
                    for (int i = 0; i < rows; i++) {
                        enq[i] = System.nanoTime();
                        batcher.add(mmsiOf(i % ships), i, flush);
                        next += intervalNanos;
                        long wait = next - System.nanoTime();
                        if (wait > 0) {
                            LockSupport.parkNanos(wait);
                        }
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
                    assertEquals(rows, BenchmarkFixtures.count(f.jt, "zncb_engine_data"), "H2 必须落下全部行");
                    if (!warmup) {
                        for (long nanos : submitNanos) {
                            s.recordLatency("submit_batch", nanos);
                        }
                        for (long nanos : e2eNanos) {
                            s.recordLatency("e2e", nanos);
                        }
                        long completedDelta = metrics.getCompletedTaskCount() - completedBase;
                        assertEquals(expectedTasks, completedDelta + policy.getRejectCount(), "批量任务无丢弃");
                        assertTrue(queuePeak[0] <= 500, "队列有界");
                        if (last) {
                            s.count("rows", rows)
                                    .count("engine_rows", BenchmarkFixtures.count(f.jt, "zncb_engine_data"))
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

    // ==================== 场景二：5 分钟断网补传 ====================

    private static final class OutageFixture {
        final JdbcTemplate jt;
        final DatabaseUploadPoller poller;
        final DatabaseUploadPoller.IncrementalStream stream;
        final DatabaseUploadPoller.LocalShip ship;
        final CountingMqttGateway gateway;

        OutageFixture(int rows, int pollBatch, long[] sampleIds) {
            jt = BenchmarkFixtures.newH2("outage_bench_" + System.nanoTime());
            BenchmarkFixtures.createShipTables(jt);
            for (int i = 0; i < rows; i += 2000) {
                int end = Math.min(i + 2000, rows);
                StringBuilder sb = new StringBuilder(
                        "INSERT INTO zncb_gps_data (ship_id, mmsi, sentence_type) VALUES ");
                for (int id = i + 1; id <= end; id++) {
                    if (id > i + 1) {
                        sb.append(',');
                    }
                    sb.append("('").append(SHIP_ID).append("','").append(MMSI).append("','RMC')");
                }
                jt.execute(sb.toString());
            }
            jt.update("INSERT INTO zncb_upload_cursor (stream_name, partition_key, last_uploaded_id)"
                    + " VALUES (?, '', 0)", TABLE);

            EdgeProperties properties = new EdgeProperties();
            properties.getUploader().setEnabled(true);
            properties.getUploader().getPoll().setBatchSize(pollBatch);
            gateway = new CountingMqttGateway(rows, sampleIds);
            ship = new DatabaseUploadPoller.LocalShip(SHIP_ID, MMSI);
            poller = new DatabaseUploadPoller(properties, jt, gateway.publisher());
            stream = new DatabaseUploadPoller.IncrementalStream("gps", TABLE, "nmea_gps", "nmea_gps");
        }

        long cursor() {
            Long v = jt.queryForObject(
                    "SELECT last_uploaded_id FROM zncb_upload_cursor WHERE stream_name = ?", Long.class, TABLE);
            return v == null ? 0L : v;
        }

        long backlog() {
            return BenchmarkFixtures.maxId(jt, TABLE) - cursor();
        }

        int driveToQuiescence(int cap) {
            int rounds = 0;
            while (rounds < cap) {
                long before = cursor();
                poller.uploadIncrementalStream(jt, ship, stream);
                rounds++;
                if (cursor() == before) {
                    break;
                }
            }
            return rounds;
        }
    }

    @Test
    @DisplayName("outage-5min-backfill: 75万行积压断网冻结，恢复后补传零丢失")
    void outage5minBackfill() {
        int rows = Integer.parseInt(prop("benchmark.outage.rows", "750000").trim());
        int pollBatch = Integer.parseInt(prop("benchmark.outage.pollBatch", "10000").trim());
        int failAfter = Integer.parseInt(prop("benchmark.outage.failAfter", "100000").trim());
        int warmupRows = Integer.parseInt(prop("benchmark.outage.warmupRows", "5000").trim());
        long cutId = (long) failAfter + 1;
        long[] samples = new long[]{1L, failAfter, cutId, rows / 2L, rows};
        BenchmarkRecorder.Scenario s = RECORDER.scenario("outage-5min-backfill")
                .param("rows_5min_at_2500_per_sec", rows)
                .param("poll_batch", pollBatch)
                .param("fail_after_rows", failAfter)
                .param("warmup_rows", warmupRows)
                .param("warmup_runs", 1)
                .param("measured_runs", 1)
                .note("2500点/秒×300秒=75万行积压；断网游标冻结在最后一个成功断点，恢复后从断点续传；"
                        + "75万行只跑单 measured 轮（5000行小轮 warmup），语义断言精确（游标/覆盖精确相等），"
                        + "recovery 为单样本；at-least-once，不宣称 exactly-once");
        // warmup：小规模同语义走一遍（不断言报告，只预热）
        {
            OutageFixture w = new OutageFixture(warmupRows, 1000, new long[]{1L, warmupRows});
            w.gateway.succeedFirstNThenFail(1000);
            w.driveToQuiescence(50);
            w.gateway.alwaysSucceed();
            w.driveToQuiescence(50);
            assertEquals(warmupRows, w.cursor());
            assertEquals(0, w.backlog());
        }
        // measured：75 万行全量
        OutageFixture f = new OutageFixture(rows, pollBatch, samples);
        s.count("initial_backlog", f.backlog());
        assertEquals(rows, f.backlog());

        f.gateway.succeedFirstNThenFail(failAfter);
        int attemptsBeforeOutage = f.gateway.attempts();
        f.driveToQuiescence(200);
        int outageAttempts = f.gateway.attempts() - attemptsBeforeOutage;
        int outageFailed = outageAttempts - failAfter;
        assertEquals(failAfter, f.cursor(), "断网后游标必须冻结在最后一个成功断点");
        assertEquals((long) rows - failAfter, f.backlog(), "剩余积压必须完整保留");
        // 失败次数只与批次对齐有关：切断批次内失败 1 次 + 确认冻结再失败 1 次（对齐时只有后者）
        assertTrue(outageFailed >= 1 && outageFailed <= 2, "断网期失败次数必须为 1~2 次");
        s.count("outage_cursor", f.cursor())
                .count("outage_backlog_peak", f.backlog())
                .count("outage_failed_attempts", outageFailed);

        f.gateway.alwaysSucceed();
        long t0 = System.nanoTime();
        f.driveToQuiescence(200);
        double recoveryMs = (System.nanoTime() - t0) / 1_000_000.0;
        s.measure("recovery", recoveryMs);
        int recoveryAttempts = f.gateway.attempts() - attemptsBeforeOutage - outageAttempts;
        assertEquals(rows, f.cursor(), "恢复后游标必须推进到末尾");
        assertEquals(0, f.backlog(), "恢复后积压必须归零");
        assertEquals(rows, f.gateway.covered(), "逐行覆盖满额，零丢失");
        assertEquals((long) rows - failAfter, recoveryAttempts, "恢复期每行恰好尝试一次（全成功）");
        assertEquals((long) rows + outageFailed, f.gateway.attempts(), "总 attempts = 全量成功 + 断网失败");
        s.count("final_cursor", f.cursor())
                .count("final_backlog", f.backlog())
                .count("covered_rows", f.gateway.covered())
                .count("total_attempts", f.gateway.attempts());
        // 切断行发布次数 = 断网失败次数 + 恢复成功 1 次，且 msg_id 恒定；其余采样行各 1 次
        List<String> cutIds = f.gateway.msgIdsForSample(cutId);
        assertEquals(outageFailed + 1, cutIds.size(), "切断行发布次数必须等于失败次数+恢复成功");
        assertTrue(cutIds.stream().allMatch(id -> id.equals(cutIds.get(0))), "重传 msg_id 必须恒定");
        for (long sample : new long[]{1L, failAfter, rows / 2L, rows}) {
            assertEquals(1, f.gateway.msgIdsForSample(sample).size(), "非切断采样行必须恰好发布 1 次");
        }
        s.count("cut_row_redeliveries", cutIds.size())
                .count("msg_id_stable", 1);
        RECORDER.complete(s);
    }
}
