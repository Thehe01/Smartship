package com.smartship.edge.benchmark;

import com.smartship.edge.benchmark.simulator.ModbusSimulationConfig;
import com.smartship.edge.benchmark.simulator.ModbusTcpDeviceSimulator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P1-4.1 Modbus 模拟器生命周期单元测试（正确性测试，随默认 {@code mvn test} 执行）。
 * <p>
 * 覆盖：close 后端口释放、阻塞 read 的 worker 被 close 解阻塞、连续 close 幂等。
 */
@DisplayName("P1-4.1 Integrity: Modbus simulator lifecycle")
class ModbusSimulatorLifecycleTest {

    @Test
    @DisplayName("close 后端口释放，可立即重新绑定")
    void portReleasedAfterClose() throws Exception {
        ModbusTcpDeviceSimulator sim = new ModbusTcpDeviceSimulator(ModbusSimulationConfig.normal());
        int port = sim.getPort();
        sim.close();
        assertFalse(sim.isAcceptThreadAlive(), "accept 线程必须退出");
        // 端口释放：可立即重新绑定
        try (ServerSocket rebound = new ServerSocket(port)) {
            assertEquals(port, rebound.getLocalPort());
        }
    }

    @Test
    @DisplayName("阻塞在 read 的 worker 被 close 解阻塞，连接计数归零")
    void blockedWorkerReleasedOnClose() throws Exception {
        ModbusTcpDeviceSimulator sim = new ModbusTcpDeviceSimulator(ModbusSimulationConfig.normal());
        Socket client = new Socket("127.0.0.1", sim.getPort());
        OutputStream out = client.getOutputStream();
        try {
            // 只发 3 个字节（不足 7 字节 MBAP 头），使服务端 worker 阻塞在 readFully
            out.write(new byte[]{0x00, 0x01, 0x00});
            out.flush();
            long deadline = System.currentTimeMillis() + 5000;
            while (sim.activeConnectionCount() != 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, sim.activeConnectionCount(), "服务端必须跟踪到该连接");

            sim.close();

            deadline = System.currentTimeMillis() + 5000;
            while (sim.activeConnectionCount() != 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(0, sim.activeConnectionCount(), "close 后连接计数必须归零");
            assertTrue(sim.isTerminated(), "worker 池必须终止");
            assertFalse(sim.isAcceptThreadAlive(), "accept 线程必须退出");
        } finally {
            try {
                client.close();
            } catch (Exception ignored) {
            }
            sim.close();
        }
    }

    @Test
    @DisplayName("连续 close 幂等，不得异常")
    void closeIdempotent() throws Exception {
        ModbusTcpDeviceSimulator sim = new ModbusTcpDeviceSimulator(ModbusSimulationConfig.normal());
        assertDoesNotThrow(() -> {
            sim.close();
            sim.close();
        });
    }
}
