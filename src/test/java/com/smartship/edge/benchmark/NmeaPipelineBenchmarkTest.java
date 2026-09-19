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
    @DisplayName("nmea-mixed-pipeline: 混合语句直驱解析+持久化（warmup 1 + measured 3 取 median）")
    void nmeaMixedPipeline() {
        NmeaSimulationConfig cfg = new NmeaSimulationConfig(
                0, Duration.ofSeconds(60), MMSI, 0.01, 0.01);
        BenchmarkRecorder.Scenario s = RECORDER.scenario("nmea-mixed-pipeline")
                .param("messages_per_run", messages())
                .param("throttle_seconds", 0)
                .param("noise_rate", 0.01)
                .param("invalid_checksum_rate", 0.01)
                .note("GPS/wind/depth 受 2000ms 内存聚合窗口门控；RSA 直写（节流 0 表示全部放行）");
        for (int run = 0; run < 4; run++) {
            boolean warmup = run == 0;
            NmeaDeviceSimulator sim = new NmeaDeviceSimulator(cfg);
            NmeaDeviceSimulator.GenerationResult gen = sim.generate(messages());
            try (Fixture f = new Fixture(0)) {
                long t0 = System.nanoTime();
                for (String sentence : gen.sentences()) {
                    long m0 = System.nanoTime();
                    f.parser.parse(sentence, "SIM");
                    if (!warmup) {
                        s.recordLatencyNanos(System.nanoTime() - m0);
                    }
                }
                double elapsedSec = (System.nanoTime() - t0) / 1e9;
                if (!warmup) {
                    long valid = gen.validCount();
                    long rows = f.rows("zncb_gps_data") + f.rows("zncb_wind_data")
                            + f.rows("zncb_depth_data") + f.rows("zncb_rudder_data");
                    long attempts = f.attempts("saveGps") + f.attempts("saveWind")
                            + f.attempts("saveDepth") + f.attempts("saveRudder");
                    s.count("input_messages", messages())
                            .count("valid_messages", valid)
                            .count("invalid_messages", gen.invalidCount())
                            .count("persist_attempts", attempts)
                            .count("persist_rows", rows)
                            .observePeak("write_reduction_ratio",
                                    valid == 0 ? 0.0 : 1.0 - (double) rows / valid);
                    s.addRunThroughput(valid / elapsedSec);
                }
            }
        }
        RECORDER.complete(s);
    }

    @Test
    @DisplayName("nmea-throttle-effect: 长节流窗口下的实际写入削减（实测计算）")
    void nmeaThrottleEffect() {
        NmeaSimulationConfig cfg = new NmeaSimulationConfig(
                0, Duration.ofSeconds(60), MMSI, 0.0, 0.0);
        BenchmarkRecorder.Scenario s = RECORDER.scenario("nmea-throttle-effect")
                .param("messages_per_run", messages())
                .param("throttle_seconds", 3600)
                .note("节流键按 mmsi:stream 聚合，3600s 窗口下每流首条放行；比例由实测行数/有效输入计算");
        for (int run = 0; run < 4; run++) {
            boolean warmup = run == 0;
            NmeaDeviceSimulator sim = new NmeaDeviceSimulator(cfg);
            NmeaDeviceSimulator.GenerationResult gen = sim.generate(messages());
            try (Fixture f = new Fixture(3600)) {
                long t0 = System.nanoTime();
                for (String sentence : gen.sentences()) {
                    long m0 = System.nanoTime();
                    f.parser.parse(sentence, "SIM");
                    if (!warmup) {
                        s.recordLatencyNanos(System.nanoTime() - m0);
                    }
                }
                double elapsedSec = (System.nanoTime() - t0) / 1e9;
                if (!warmup) {
                    long valid = gen.validCount();
                    long rows = f.rows("zncb_gps_data") + f.rows("zncb_wind_data")
                            + f.rows("zncb_depth_data") + f.rows("zncb_rudder_data");
                    s.count("input_messages", messages())
                            .count("valid_messages", valid)
                            .count("persist_rows", rows)
                            .observePeak("write_reduction_ratio",
                                    valid == 0 ? 0.0 : 1.0 - (double) rows / valid);
                    s.addRunThroughput(valid / elapsedSec);
                }
            }
        }
        RECORDER.complete(s);
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
                double elapsedSec = (System.nanoTime() - t0) / 1e9;
                if (!warmup) {
                    s.count("input_messages", count)
                            .count("received_lines", received.get())
                            .count("valid_messages", gen.validCount())
                            .count("invalid_messages", gen.invalidCount())
                            .count("persist_rows", f.rows("zncb_gps_data") + f.rows("zncb_wind_data")
                                    + f.rows("zncb_depth_data") + f.rows("zncb_rudder_data"));
                    for (long nanos : lineNanos) {
                        s.recordLatencyNanos(nanos);
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
