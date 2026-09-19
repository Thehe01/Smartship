package com.smartship.edge.collector;

import com.smartship.edge.collect.modbus.ModbusDataHandler;
import com.smartship.edge.collect.modbus.ModbusParser;
import com.smartship.edge.collect.modbus.tcp.ModbusProtocolException;
import com.smartship.edge.collect.modbus.tcp.ModbusTcpResponse;
import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PersistenceThrottle;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * P1-3.2 Collector Production Wiring Closure 生产装配测试
 * <p>
 * 验证生产创建 SocketPollingCollector 必经 CollectorFactory 并自动获得
 * Spring 管理的 ModbusParser，打通 Modbus TCP -&gt; Parser -&gt; Handler -&gt; Persistence 全链路。
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = CollectorFactoryTest.TestConfig.class)
class CollectorFactoryTest {

    @Configuration
    static class TestConfig {

        @Bean
        EdgeProperties edgeProperties() {
            EdgeProperties properties = new EdgeProperties();
            properties.setMmsi("413999999");
            properties.getCollect().getPersist().setEnabled(true);
            properties.getCollect().getPersist().setMinWriteIntervalSeconds(0);
            return properties;
        }

        @Bean
        PersistenceThrottle persistenceThrottle(EdgeProperties edgeProperties) {
            return new PersistenceThrottle(edgeProperties);
        }

        @Bean
        NmeaDataPersistenceService persistenceService() {
            return mock(NmeaDataPersistenceService.class);
        }

        @Bean
        ModbusDataHandler modbusDataHandler(NmeaDataPersistenceService persistenceService,
                                            EdgeProperties edgeProperties,
                                            PersistenceThrottle persistenceThrottle) {
            return new ModbusDataHandler(persistenceService, edgeProperties, persistenceThrottle);
        }

        @Bean
        ModbusParser modbusParser(ModbusDataHandler modbusDataHandler) {
            return new ModbusParser(modbusDataHandler);
        }

        @Bean
        CollectorFactory collectorFactory(ModbusParser modbusParser) {
            return new CollectorFactory(modbusParser);
        }
    }

    @Autowired
    private CollectorFactory collectorFactory;

    @Autowired
    private ModbusParser modbusParser;

    @Autowired
    private NmeaDataPersistenceService persistenceService;

    /**
     * 构建包含 16 个寄存器的合法 Modbus TCP 0x03 响应帧
     */
    private static byte[] buildValid0x03Response(int txId, int unitId) {
        int byteCount = 32; // 16 registers * 2
        int length = 1 + 1 + 1 + byteCount; // unitId(1) + fc(1) + byteCount(1) + data(32)
        byte[] frame = new byte[6 + length];

        frame[0] = (byte) ((txId >> 8) & 0xFF);
        frame[1] = (byte) (txId & 0xFF);
        frame[2] = 0x00;
        frame[3] = 0x00;
        frame[4] = (byte) ((length >> 8) & 0xFF);
        frame[5] = (byte) (length & 0xFF);

        frame[6] = (byte) (unitId & 0xFF);
        frame[7] = 0x03;
        frame[8] = (byte) byteCount;

        for (int i = 0; i < 16; i++) {
            int val = (i + 1) * 100;
            frame[9 + i * 2] = (byte) ((val >> 8) & 0xFF);
            frame[10 + i * 2] = (byte) (val & 0xFF);
        }
        return frame;
    }

    /**
     * 构建 Modbus 异常响应报文 (0x83)
     */
    private static byte[] buildExceptionResponse(int txId, int unitId, int exceptionCode) {
        int length = 3; // unitId(1) + fc(1) + exceptionCode(1)
        byte[] frame = new byte[6 + length];
        frame[0] = (byte) ((txId >> 8) & 0xFF);
        frame[1] = (byte) (txId & 0xFF);
        frame[2] = 0x00;
        frame[3] = 0x00;
        frame[4] = 0x00;
        frame[5] = (byte) length;

        frame[6] = (byte) (unitId & 0xFF);
        frame[7] = (byte) 0x83; // 0x03 | 0x80
        frame[8] = (byte) (exceptionCode & 0xFF);
        return frame;
    }

    @Test
    @DisplayName("Test 1: Spring Factory 正常装配，创建的 Collector 自动持有 ModbusParser 且可正常 init")
    void testFactoryWiresParserAndCollectorCanInit() {
        assertNotNull(collectorFactory);
        assertNotNull(modbusParser);

        SocketPollingCollector collector = collectorFactory.createPollingCollector();
        try {
            assertNotNull(collector);
            assertNotNull(collector.getModbusParser(), "Factory 创建的 Collector 必须自动装配 ModbusParser");
            assertSame(modbusParser, collector.getModbusParser(), "必须是 Spring 管理的同一个 ModbusParser Bean");

            // 生产 init 不抛异常（fail-fast 不误伤正常装配）
            assertDoesNotThrow(() -> collector.init(null, null, List.of(), List.of()));
        } finally {
            collector.close();
        }
    }

    @Test
    @DisplayName("Test 2: 多实例独立（per-device），共享同一个 Parser Bean")
    void testCollectorsAreIndependentInstancesSharingParser() {
        SocketPollingCollector collector1 = collectorFactory.createPollingCollector();
        SocketPollingCollector collector2 = collectorFactory.createPollingCollector();

        assertNotSame(collector1, collector2, "每次创建必须返回独立 Collector 实例");
        assertNotSame(collector1.getResponseQueue(), collector2.getResponseQueue(), "实例状态（响应队列）必须隔离");
        assertSame(collector1.getModbusParser(), collector2.getModbusParser(),
                "所有实例必须共享同一个 Spring ModbusParser");
        assertSame(modbusParser, collector1.getModbusParser());
    }

    @Test
    @DisplayName("Test 3: Factory 创建的 Collector 真实打通 Parser -> Handler -> saveEngine 业务链")
    void testFactoryCollectorEndToEndToPersistence() throws Exception {
        SocketPollingCollector collector = collectorFactory.createPollingCollector();
        // 仅测试观测需要包裹 spy，Collector 实例本身来自 Factory，生产装配路径不变
        ModbusParser spiedParser = spy(collector.getModbusParser());
        collector.setModbusParser(spiedParser);

        int expectedTxId = collector.getTransactionCount();
        byte[] validFrame = buildValid0x03Response(expectedTxId, 1);

        ModbusTcpResponse response = collector.pollOnce(
                new ByteArrayInputStream(validFrame), new ByteArrayOutputStream());
        assertNotNull(response);
        assertFalse(response.isException());
        assertEquals(16, response.registers().length);

        collector.processResponse(response);

        verify(spiedParser, times(1)).parse(eq(1), argThat(pdu -> pdu != null && (pdu[0] & 0xFF) == 0x03));
        verify(persistenceService, times(1)).saveEngine(
                eq("413999999"),
                eq(1),
                eq(10.0), // rpm = 100 / 10.0
                eq(20.0), // coolantTemp = 200 / 10.0
                anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyDouble(),
                anyDouble(), anyDouble(), anyInt(), anyInt(), anyInt(), anyInt()
        );
    }

    @Test
    @DisplayName("Test 4: 无 Parser 的 Collector 执行生产 init 必须 fail-fast，杜绝静默丢数据")
    void testMissingParserFailsFastOnInit() {
        SocketPollingCollector bare = new SocketPollingCollector();
        try {
            assertNull(bare.getModbusParser());
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> bare.init(null, null, List.of(), List.of()),
                    "缺失 ModbusParser 的生产 init 必须抛 IllegalStateException，而不是静默只打印 registers");
            assertTrue(ex.getMessage().contains("ModbusParser"));
        } finally {
            bare.close();
        }
    }

    @Test
    @DisplayName("Test 5: txId/unitId/function 不匹配与异常报文均不得进入业务层")
    void testAbnormalResponsesNeverReachBusinessLayer() throws Exception {
        // persistenceService 为 Spring 共享 mock，先清除其他测试的历史调用，保证 never 断言只统计本测试
        clearInvocations(persistenceService);

        SocketPollingCollector collector = collectorFactory.createPollingCollector();
        ModbusParser spiedParser = spy(collector.getModbusParser());
        collector.setModbusParser(spiedParser);

        // 1. txId mismatch
        int expectedTxId = collector.getTransactionCount();
        byte[] txMismatch = buildValid0x03Response(expectedTxId + 100, 1);
        assertThrows(ModbusProtocolException.class, () -> collector.pollOnce(
                new ByteArrayInputStream(txMismatch), new ByteArrayOutputStream()));

        // 2. unitId mismatch
        expectedTxId = collector.getTransactionCount();
        byte[] unitMismatch = buildValid0x03Response(expectedTxId, 2);
        assertThrows(ModbusProtocolException.class, () -> collector.pollOnce(
                new ByteArrayInputStream(unitMismatch), new ByteArrayOutputStream()));

        // 3. function mismatch（请求 0x03，响应 0x04）
        expectedTxId = collector.getTransactionCount();
        byte[] funcMismatch = buildValid0x03Response(expectedTxId, 1);
        funcMismatch[7] = 0x04;
        assertThrows(ModbusProtocolException.class, () -> collector.pollOnce(
                new ByteArrayInputStream(funcMismatch), new ByteArrayOutputStream()));

        // 4. Modbus exception response（0x83）：解码通过但不得进入业务层
        expectedTxId = collector.getTransactionCount();
        byte[] exceptionFrame = buildExceptionResponse(expectedTxId, 1, 0x02);
        ModbusTcpResponse exceptionResponse = collector.pollOnce(
                new ByteArrayInputStream(exceptionFrame), new ByteArrayOutputStream());
        assertTrue(exceptionResponse.isException());
        collector.processResponse(exceptionResponse);

        verify(spiedParser, never()).parse(anyInt(), any());
        verify(persistenceService, never()).saveEngine(
                anyString(), anyInt(), anyDouble(), anyDouble(), anyDouble(), anyDouble(),
                anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyInt(), anyInt(), anyInt(), anyInt()
        );
    }
}
