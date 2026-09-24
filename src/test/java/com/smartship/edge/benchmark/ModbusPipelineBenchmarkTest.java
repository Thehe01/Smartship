package com.smartship.edge.benchmark;

import com.smartship.edge.benchmark.simulator.ModbusSimulationConfig;
import com.smartship.edge.benchmark.simulator.ModbusTcpDeviceSimulator;
import com.smartship.edge.benchmark.support.BenchmarkFixtures;
import com.smartship.edge.collect.modbus.ModbusDataHandler;
import com.smartship.edge.collect.modbus.ModbusParser;
import com.smartship.edge.collector.CollectorFactory;
import com.smartship.edge.collector.SocketPollingCollector;
import com.smartship.edge.collector.model.BaseInfoVO;
import com.smartship.edge.collector.model.ConfigDevice;
import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PersistenceThrottle;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * P1-4 Modbus TCP 采集流水线 Benchmark（integration benchmark，非 JMH）。
 * <p>
 * 主路径真实经过 {@code CollectorFactory → SocketPollingCollector → 真实 Socket →
 * 模拟器 → MBAP Codec → responseQueue → ModbusParser → ModbusDataHandler → H2}，
 * 不以直接调用 {@code processResponse()} 作为唯一测量；延迟另用同步 pollOnce 回环补充。
 * 恢复语义只报告源码真实具备的行为（无自动重连，恢复需外部 {@code connect()}）。
 */
@Tag("benchmark")
@DisplayName("P1-4 Benchmark: Modbus pipeline")
class ModbusPipelineBenchmarkTest {

    private static final String MMSI = "413999999";
    private static final BenchmarkRecorder RECORDER = new BenchmarkRecorder(params());

    private static Map<String, String> params() {
        return Map.of(
                "benchmark.durationSeconds", prop("benchmark.durationSeconds", "5"),
                "benchmark.threads", prop("benchmark.threads", "32"));
    }

    private static String prop(String key, String def) {
        return System.getProperty(key, def);
    }

    private static int durationSeconds() {
        return Integer.parseInt(prop("benchmark.durationSeconds", "5"));
    }

    @AfterAll
    static void flushReport() {
        RECORDER.flush();
    }

    /** 单轮夹具：全新 H2 船库 + 真实 Parser/Handler + spy 持久化（行数以 H2 为准）。 */
    private static final class Fixture {
        final JdbcTemplate jt;
        final ModbusParser realParser;
        final ModbusParser spiedParser;
        final NmeaDataPersistenceService persistSpy;
        final CollectorFactory factory;

        Fixture() {
            jt = BenchmarkFixtures.newH2("modbus_bench_" + System.nanoTime());
            BenchmarkFixtures.createShipTables(jt);
            EdgeProperties properties = new EdgeProperties();
            properties.setMmsi(MMSI);
            properties.setSchemaReady(true);
            properties.getCollect().getPersist().setEnabled(true);
            properties.getCollect().getPersist().setMinWriteIntervalSeconds(0);

            NmeaDataPersistenceService realPersist =
                    new NmeaDataPersistenceService(jt, properties, new PersistenceThrottle(properties));
            persistSpy = spy(realPersist);
            ModbusDataHandler handler = new ModbusDataHandler(
                    persistSpy, properties, new PersistenceThrottle(properties));
            realParser = new ModbusParser(handler);
            spiedParser = spy(realParser);
            factory = new CollectorFactory(spiedParser);
        }

        long parseCount() {
            return Mockito.mockingDetails(spiedParser).getInvocations().stream()
                    .filter(i -> i.getMethod().getName().equals("parse"))
                    .count();
        }

        long engineRows() {
            return BenchmarkFixtures.count(jt, "zncb_engine_data");
        }

        long persistCalls() {
            return Mockito.mockingDetails(persistSpy).getInvocations().stream()
                    .filter(i -> i.getMethod().getName().equals("saveEngine"))
                    .count();
        }
    }

    private static ConfigDevice deviceFor(int port) {
        return ConfigDevice.builder()
                .deviceCode(9001L)
                .deviceName("SIM-MODBUS")
                .deviceType("modbus-tcp")
                .baseInfo(BaseInfoVO.builder().ip("127.0.0.1").port(port).build())
                .build();
    }

    @Test
    @DisplayName("modbus-normal: 单设备真实轮询全链路 + 同步回环延迟")
    void modbusNormal() throws Exception {
        BenchmarkRecorder.Scenario s = RECORDER.scenario("modbus-normal")
                .param("devices", 1)
                .param("poll_interval_ms", 5)
                .param("duration_seconds", durationSeconds())
                .param("warmup_runs", 1)
                .param("measured_runs", 3)
                .param("sync_roundtrips_per_run", 200)
                .note("主路径为 Collector 异步线程真实 Socket 轮询；roundtrip 延迟由同步 pollOnce 回环补充测量；"
                        + "计数器取末轮 measured 单轮值");
        // warmup + measured 共 4 轮，每轮独立 simulator 与夹具，保证隔离
        for (int run = 0; run < 4; run++) {
            boolean warmup = run == 0;
            boolean last = run == 3;
            try (ModbusTcpDeviceSimulator sim = new ModbusTcpDeviceSimulator(ModbusSimulationConfig.normal())) {
                Fixture f = new Fixture();
                SocketPollingCollector collector = f.factory.createPollingCollector();
                collector.setPollIntervalMs(5);
                ConfigDevice device = deviceFor(sim.getPort());
                try {
                    // 注意顺序：先 init（装配 device 并启动线程），再 connect（建链）；
                    // connect 在 init 之前调用会因 device 为空而静默无建链
                    collector.init(null, device, List.of(), List.of());
                    collector.connect();
                    Thread.sleep(durationSeconds() * 1000L);
                } finally {
                    collector.close();
                }
                if (!warmup) {
                    long attempted = collector.getTransactionCount() - 1;
                    long parsed = f.parseCount();
                    long rows = f.engineRows();
                    assertTrue(attempted > 0, "异步轮询必须实际发起请求，否则测量空洞");
                    assertTrue(parsed > 0, "正常场景必须解析出合法响应");
                    if (last) {
                        s.count("requests_attempted", attempted)
                                .count("valid_responses_parsed", parsed)
                                .count("persist_calls", f.persistCalls())
                                .count("engine_rows", rows)
                                .count("errors", Math.max(0, attempted - parsed));
                    }
                    s.addRunThroughput(parsed / (double) durationSeconds());
                }
            }
            // 同步回环延迟（ supplementary，非唯一指标；每轮独立连接）
            try (ModbusTcpDeviceSimulator sim = new ModbusTcpDeviceSimulator(ModbusSimulationConfig.normal());
                 Socket socket = new Socket("127.0.0.1", sim.getPort())) {
                Fixture f = new Fixture();
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                SocketPollingCollector probe = new SocketPollingCollector(f.realParser);
                for (int i = 0; i < 200; i++) {
                    long m0 = System.nanoTime();
                    probe.pollOnce(in, out);
                    if (!warmup) {
                        s.recordLatency("roundtrip", System.nanoTime() - m0);
                    }
                }
                if (last) {
                    s.count("sync_roundtrips", 200);
                }
            }
        }
        RECORDER.complete(s);
    }

    @Test
    @DisplayName("modbus-fault-exception: 100% 异常响应零入库")
    void modbusFaultException() throws Exception {
        runFaultScenario("modbus-fault-exception",
                ModbusSimulationConfig.faulty(0, 0, 1.0, 0, 0), 2000);
    }

    @Test
    @DisplayName("modbus-fault-txid: 100% txId 错误零入库")
    void modbusFaultTxId() throws Exception {
        runFaultScenario("modbus-fault-txid",
                ModbusSimulationConfig.faulty(0, 0, 0, 1.0, 0), 2000);
    }

    @Test
    @DisplayName("modbus-fault-malformed: 畸形帧零入库")
    void modbusFaultMalformed() throws Exception {
        runFaultScenario("modbus-fault-malformed",
                ModbusSimulationConfig.faulty(0, 0, 0, 0, 1.0), 2000);
    }

    private void runFaultScenario(String name, ModbusSimulationConfig cfg, long runMs) throws Exception {
        BenchmarkRecorder.Scenario s = RECORDER.scenario(name)
                .param("run_ms", runMs)
                .note("确定性 100% 故障注入；invalid response 必须 0 persistence writes");
        try (ModbusTcpDeviceSimulator sim = new ModbusTcpDeviceSimulator(cfg)) {
            Fixture f = new Fixture();
            SocketPollingCollector collector = f.factory.createPollingCollector();
            collector.setPollIntervalMs(5);
            try {
                collector.init(null, deviceFor(sim.getPort()), List.of(), List.of());
                collector.connect();
                Thread.sleep(runMs);
            } finally {
                collector.close();
            }
            long attempted = collector.getTransactionCount() - 1;
            assertTrue(attempted > 0, name + ": 必须实际发起请求，否则故障注入未生效");
            s.count("requests_attempted", attempted)
                    .count("simulator_requests_seen", sim.requestsReceived())
                    .count("valid_responses_parsed", f.parseCount())
                    .count("persist_calls", f.persistCalls())
                    .count("engine_rows", f.engineRows());
            assertEquals(0, f.parseCount(), name + ": 异常响应不得进入 Parser");
            assertEquals(0, f.engineRows(), name + ": 异常响应不得入库");
        }
        RECORDER.complete(s);
    }

    @Test
    @DisplayName("modbus-fault-disconnect-reconnect: 断开零入库，外部重连后恢复（无自动重连）")
    void modbusFaultDisconnectReconnect() throws Exception {
        BenchmarkRecorder.Scenario s = RECORDER.scenario("modbus-fault-disconnect-reconnect")
                .param("warmup_runs", 1)
                .param("measured_runs", 3)
                .param("outage_ms", 1500)
                .note("当前 Collector 无自动重连：断开后轮询线程存活但停止消费；"
                        + "恢复必须由外部 connect() 触发；recovery 为 connect 到首次解析的真实耗时；"
                        + "计数器取末轮 measured 单轮值");
        for (int run = 0; run < 4; run++) {
            boolean warmup = run == 0;
            boolean last = run == 3;
            // 阶段一：必断模拟器
            ModbusTcpDeviceSimulator badSim =
                    new ModbusTcpDeviceSimulator(ModbusSimulationConfig.faulty(0, 1.0, 0, 0, 0));
            Fixture f = new Fixture();
            SocketPollingCollector collector = f.factory.createPollingCollector();
            collector.setPollIntervalMs(5);
            ConfigDevice device = deviceFor(badSim.getPort());
            try {
                collector.init(null, device, List.of(), List.of());
                collector.connect();
                Thread.sleep(1500);
                long parsedDuringOutage = f.parseCount();
                if (!warmup && last) {
                    s.count("outage_parsed", parsedDuringOutage)
                            .count("outage_engine_rows", f.engineRows())
                            .count("simulator_disconnects", badSim.disconnects());
                }
                assertTrue(badSim.disconnects() > 0, "模拟器必须实际执行过断开注入");
                assertEquals(0, parsedDuringOutage, "断开期间不得解析");
                assertEquals(0, f.engineRows(), "断开期间不得入库");

                // 阶段二：换正常模拟器 + 外部重连，测量到首次解析的真实恢复耗时
                badSim.close();
                try (ModbusTcpDeviceSimulator goodSim =
                             new ModbusTcpDeviceSimulator(ModbusSimulationConfig.normal())) {
                    device.getBaseInfo().setPort(goodSim.getPort());
                    long t0 = System.nanoTime();
                    collector.connect();
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (f.parseCount() == 0 && System.currentTimeMillis() < deadline) {
                        Thread.sleep(50);
                    }
                    double recoveryMs = (System.nanoTime() - t0) / 1_000_000.0;
                    long parsedAfter = f.parseCount();
                    if (!warmup) {
                        s.measure("recovery", recoveryMs);
                        if (last) {
                            s.count("recovered_parsed_total", parsedAfter)
                                    .count("recovered_engine_rows", f.engineRows())
                                    .observePeak("recovery_ms_last_run", recoveryMs);
                        }
                    }
                    assertTrue(parsedAfter > 0, "外部重连后必须恢复消费");
                    assertTrue(f.engineRows() > 0, "外部重连后必须恢复入库");
                }
            } finally {
                collector.close();
                badSim.close();
            }
        }
        RECORDER.complete(s);
    }

    @Test
    @DisplayName("modbus-fault-timeout: 响应超时零入库（延迟大于 SoTimeout）")
    void modbusFaultTimeout() throws Exception {
        BenchmarkRecorder.Scenario s = RECORDER.scenario("modbus-fault-timeout")
                .param("response_delay_ms", 3500)
                .param("socket_timeout_ms", 3000)
                .note("模拟器延迟 3.5s > SoTimeout 3s，单次同步 poll 必须超时且零入库");
        try (ModbusTcpDeviceSimulator sim = new ModbusTcpDeviceSimulator(
                ModbusSimulationConfig.faulty(3500, 0, 0, 0, 0));
             Socket socket = new Socket("127.0.0.1", sim.getPort())) {
            Fixture f = new Fixture();
            socket.setSoTimeout(3000);
            SocketPollingCollector probe = new SocketPollingCollector(f.realParser);
            assertThrows(IOException.class,
                    () -> probe.pollOnce(socket.getInputStream(), socket.getOutputStream()));
            s.count("timeout_errors", 1).count("engine_rows", f.engineRows());
            assertEquals(0, f.engineRows());
        }
        RECORDER.complete(s);
    }
}
