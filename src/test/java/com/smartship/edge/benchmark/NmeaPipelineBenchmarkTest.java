package com.smartship.edge.benchmark;

import com.smartship.edge.benchmark.simulator.NmeaDeviceSimulator;
import com.smartship.edge.benchmark.simulator.NmeaSimulationConfig;
import com.smartship.edge.benchmark.support.BenchmarkFixtures;
import com.smartship.edge.collect.nmea.parser.NmeaParser;
import com.smartship.edge.collect.nmea.service.NmeaDataHandler;
import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PersistenceThrottle;
import com.smartship.edge.routing.ShipDataSourceManager;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import com.smartship.edge.uploader.mqtt.MqttClientManager;
import com.smartship.edge.routing.ShipAutoRegisterService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * P1-4 NMEA 采集流水线 Benchmark（integration benchmark，非 JMH）。
 * <p>
 * 真实驱动 {@code NmeaParser → NmeaDataHandler → PersistenceThrottle →
 * NmeaDataPersistenceService → H2}，测量解析延迟、吞吐与 DB 写入削减。
 * 所有削减比例均由实测数字计算，不预设结论。
 */
@Tag("benchmark")
@DisplayName("P1-4 Benchmark: NMEA pipeline")
class NmeaPipelineBenchmarkTest {

    private static final String MMSI = "413999999";
    private static final BenchmarkRecorder RECORDER = new BenchmarkRecorder(params());

    private static Map<String, String> params() {
        return Map.of(
                "benchmark.messages", prop("benchmark.messages", "3000"),
                "benchmark.threads", prop("benchmark.threads", "32"));
    }

    private static String prop(String key, String def) {
        return System.getProperty(key, def);
    }

    private static int messages() {
        return Integer.parseInt(prop("benchmark.messages", "3000"));
    }

    @AfterAll
    static void flushReport() {
        RECORDER.flush();
    }

    /** 单轮夹具：全新 H2 船库 + 全真实链路（persistence 用 spy 包裹以统计尝试次数）。 */
    private static final class Fixture implements AutoCloseable {
        final JdbcTemplate jt;
        final EdgeProperties properties;
        final NmeaDataPersistenceService persistSpy;
        final NmeaParser parser;

        Fixture(int throttleSeconds) {
            jt = BenchmarkFixtures.newH2("nmea_bench_" + System.nanoTime());
            BenchmarkFixtures.createShipTables(jt);
            properties = new EdgeProperties();
            properties.setMmsi(MMSI);
            properties.setSchemaReady(true);
            properties.getCollect().getPersist().setEnabled(true);
            properties.getCollect().getPersist().setMinWriteIntervalSeconds(throttleSeconds);

            ShipDataSourceManager manager = mock(ShipDataSourceManager.class);
            when(manager.getJdbcTemplate(any(), eq(MMSI))).thenReturn(jt);
            NmeaDataPersistenceService real =
                    new NmeaDataPersistenceService(manager, properties, new PersistenceThrottle(properties));
            persistSpy = spy(real);
            NmeaDataHandler handler = new NmeaDataHandler(persistSpy, new PersistenceThrottle(properties));
            parser = new NmeaParser(handler, persistSpy, properties,
                    mock(ShipAutoRegisterService.class), mock(MqttClientManager.class));
        }

        long attempts(String method) {
            return Mockito.mockingDetails(persistSpy).getInvocations().stream()
                    .filter(i -> i.getMethod().getName().equals(method))
                    .count();
        }

        long rows(String table) {
            return BenchmarkFixtures.count(jt, table);
        }

        @Override
        public void close() {
            clearInvocations(persistSpy);
        }
    }

    @Test
    @DisplayName("nmea-ab: 同一确定性 dataset 的 Baseline(throttle=0) vs Throttled(throttle=3600)")
    void nmeaAbThrottle() {
        // 同一 dataset 复用两臂：固定 seed 生成一次，List 直接复用，不重新 Random
        NmeaSimulationConfig cfg = new NmeaSimulationConfig(
                0, Duration.ofSeconds(60), MMSI, 0.01, 0.01);
        NmeaDeviceSimulator.GenerationResult generated =
                new NmeaDeviceSimulator(cfg).generate(messages());
        List<String> dataset = List.copyOf(generated.sentences());
        int validQuota = generated.validCount();

        BenchmarkRecorder.Scenario baseline = RECORDER.scenario("nmea-ab-baseline")
                .param("messages", dataset.size())
                .param("dataset_seed", cfg.seed())
                .param("throttle_seconds", 0)
                .param("warmup_runs", 1)
                .param("measured_runs", 3)
                .count("input_messages", dataset.size())
                .count("input_valid_messages", validQuota)
                .note("同一 dataset 复用两臂；GPS/wind/depth 受 2000ms 聚合窗口门控；"
                        + "计数器取末轮 measured 单轮值，latency/throughput 覆盖 3 轮");
        long[] baselineRows = new long[3];
        for (int run = 0; run < 4; run++) {
            boolean warmup = run == 0;
            boolean last = run == 3;
            long[] acc = runDataset(dataset, baseline, 0, warmup);
            if (!warmup) {
                baselineRows[run - 1] = acc[0];
                baseline.addRunThroughput(validQuota / (acc[2] / 1e9));
                if (last) {
                    baseline.count("persist_attempts", acc[1])
                            .count("persist_rows", acc[0]);
                }
            }
        }
        RECORDER.complete(baseline);

        BenchmarkRecorder.Scenario throttled = RECORDER.scenario("nmea-ab-throttled")
                .param("messages", dataset.size())
                .param("dataset_seed", cfg.seed())
                .param("throttle_seconds", 3600)
                .param("warmup_runs", 1)
                .param("measured_runs", 3)
                .count("input_messages", dataset.size())
                .count("input_valid_messages", validQuota)
                .note("input→baseline 差异含协议语义与聚合窗口影响；"
                        + "baseline→throttled 差异才代表 PersistenceThrottle 额外削减；"
                        + "ratio 按轮配对计算后取 median");
        long[] throttledRows = new long[3];
        for (int run = 0; run < 4; run++) {
            boolean warmup = run == 0;
            boolean last = run == 3;
            long[] acc = runDataset(dataset, throttled, 3600, warmup);
            if (!warmup) {
                throttledRows[run - 1] = acc[0];
                throttled.addRunThroughput(validQuota / (acc[2] / 1e9));
                if (last) {
                    throttled.count("persist_attempts", acc[1])
                            .count("persist_rows", acc[0])
                            .count("baseline_persist_rows", baselineRows[run - 1]);
                }
            }
        }
        int ratioPairs = 0;
        for (int i = 0; i < 3; i++) {
            Double ratio = BenchmarkRecorder.throttleReductionRatio(baselineRows[i], throttledRows[i]);
            if (ratio != null) {
                throttled.measure("throttle_write_reduction_ratio", ratio);
                ratioPairs++;
            }
        }
        if (ratioPairs == 0) {
            throttled.note("throttle_write_reduction_ratio: N/A（baseline_rows == 0）");
        }
        RECORDER.complete(throttled);
        // 口径断言：节流臂入库数不得超过基线臂（同 dataset 下单调性）
        for (int i = 0; i < 3; i++) {
            assertTrue(throttledRows[i] <= baselineRows[i],
                    "同 dataset 下 throttled_rows 必须 <= baseline_rows");
        }
    }

    /**
     * 用全新夹具跑一遍 dataset。
     *
     * @return {persistRows, persistAttempts, elapsedNanos}
     */
    private static long[] runDataset(List<String> dataset, BenchmarkRecorder.Scenario s,
                                     int throttleSeconds, boolean warmup) {
        try (Fixture f = new Fixture(throttleSeconds)) {
            long t0 = System.nanoTime();
            for (String sentence : dataset) {
                long m0 = System.nanoTime();
                f.parser.parse(sentence, "SIM");
                if (!warmup) {
                    s.recordLatency("parse", System.nanoTime() - m0);
                }
            }
            long elapsed = System.nanoTime() - t0;
            long rows = f.rows("zncb_gps_data") + f.rows("zncb_wind_data")
                    + f.rows("zncb_depth_data") + f.rows("zncb_rudder_data");
            long attempts = f.attempts("saveGps") + f.attempts("saveWind")
                    + f.attempts("saveDepth") + f.attempts("saveRudder");
            return new long[]{rows, attempts, elapsed};
        }
    }

    @Test
    @DisplayName("nmea-tcp-streaming: 真实 Socket 流式读取 + 解析（坏校验零入库）")
    void nmeaTcpStreaming() throws Exception {
        int count = Math.min(messages(), 2000);
        NmeaSimulationConfig cfg = new NmeaSimulationConfig(
                5000, Duration.ofSeconds(60), MMSI, 0.01, 0.01);
        NmeaDeviceSimulator sim = new NmeaDeviceSimulator(cfg);
        NmeaDeviceSimulator.GenerationResult gen = sim.generate(count);

        BenchmarkRecorder.Scenario s = RECORDER.scenario("nmea-tcp-streaming")
                .param("messages_per_run", count)
                .param("stream_rate_per_sec", 5000)
                .note("服务端逐行读取真实 TCP 流并 parse；坏校验语句必须零入库");
        for (int run = 0; run < 4; run++) {
            boolean warmup = run == 0;
            boolean last = run == 3;
            try (Fixture f = new Fixture(0);
                 ServerSocket server = new ServerSocket(0, 50,
                         java.net.InetAddress.getByName("127.0.0.1"))) {
                int port = server.getLocalPort();
                CountDownLatch done = new CountDownLatch(1);
                AtomicInteger received = new AtomicInteger();
                List<Long> lineNanos = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
                Thread reader = new Thread(() -> {
                    try (Socket socket = server.accept();
                         BufferedReader br = new BufferedReader(new InputStreamReader(
                                 socket.getInputStream(), StandardCharsets.US_ASCII))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            long m0 = System.nanoTime();
                            f.parser.parse(line, "TCP-SIM");
                            lineNanos.add(System.nanoTime() - m0);
                            received.incrementAndGet();
                        }
                    } catch (Exception ignored) {
                    } finally {
                        done.countDown();
                    }
                }, "nmea-tcp-reader");
                reader.setDaemon(true);
                reader.start();

                long t0 = System.nanoTime();
                try (Socket client = new Socket("127.0.0.1", port);
                     PrintWriter pw = new PrintWriter(client.getOutputStream(), true,
                             StandardCharsets.US_ASCII)) {
                    for (String sentence : gen.sentences()) {
                        pw.println(sentence);
                    }
                }
                assertTrue(done.await(30, TimeUnit.SECONDS), "TCP 流必须在 30s 内消费完毕");
                reader.join(5000);
                assertFalse(reader.isAlive(), "reader 线程必须在流结束后退出，不得仅依赖 daemon");
                double elapsedSec = (System.nanoTime() - t0) / 1e9;
                if (!warmup) {
                    if (last) {
                        s.count("input_messages", count)
                                .count("received_lines", received.get())
                                .count("valid_messages", gen.validCount())
                                .count("invalid_messages", gen.invalidCount())
                                .count("persist_rows", f.rows("zncb_gps_data") + f.rows("zncb_wind_data")
                                        + f.rows("zncb_depth_data") + f.rows("zncb_rudder_data"));
                    }
                    for (long nanos : lineNanos) {
                        s.recordLatency("parse", nanos);
                    }
                    s.addRunThroughput(received.get() / elapsedSec);
                }
            }
        }
        // 确定性断言：坏校验语句零入库（行数不可能超过有效语句数，且纯坏校验流零增长）
        try (Fixture f = new Fixture(0)) {
            long before = f.rows("zncb_rudder_data") + f.rows("zncb_gps_data")
                    + f.rows("zncb_wind_data") + f.rows("zncb_depth_data");
            NmeaDeviceSimulator bad = new NmeaDeviceSimulator(new NmeaSimulationConfig(
                    0, Duration.ofSeconds(10), MMSI, 0.5, 0.5));
            List<String> invalidOnly = bad.generate(200).sentences().stream()
                    .filter(x -> !isChecksumValid(x))
                    .toList();
            assertFalse(invalidOnly.isEmpty());
            for (String sentence : invalidOnly) {
                f.parser.parse(sentence, "SIM-BAD");
            }
            long after = f.rows("zncb_rudder_data") + f.rows("zncb_gps_data")
                    + f.rows("zncb_wind_data") + f.rows("zncb_depth_data");
            assertEquals(before, after, "坏校验语句必须零入库");
            s.count("invalid_only_rejected", invalidOnly.size());
        }
        RECORDER.complete(s);
    }

    private static boolean isChecksumValid(String s) {
        int star = s.lastIndexOf('*');
        if (star < 0 || star + 2 >= s.length()) {
            return false;
        }
        int calc = 0;
        for (int i = 1; i < star; i++) {
            calc ^= s.charAt(i);
        }
        try {
            return calc == Integer.parseInt(s.substring(star + 1, star + 3), 16);
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
