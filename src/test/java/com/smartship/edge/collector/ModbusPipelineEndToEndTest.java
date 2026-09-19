package com.smartship.edge.collector;

import com.smartship.edge.collect.modbus.ModbusDataHandler;
import com.smartship.edge.collect.modbus.ModbusParser;
import com.smartship.edge.collect.modbus.tcp.ModbusProtocolException;
import com.smartship.edge.collect.modbus.tcp.ModbusTcpResponse;
import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PersistenceThrottle;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ModbusPipelineEndToEndTest {

    private NmeaDataPersistenceService persistenceService;
    private EdgeProperties properties;
    private PersistenceThrottle throttle;
    private ModbusDataHandler dataHandler;
    private ModbusParser modbusParser;
    private ModbusParser spiedParser;
    private SocketPollingCollector collector;

    @BeforeEach
    void setUp() {
        persistenceService = mock(NmeaDataPersistenceService.class);
        properties = new EdgeProperties();
        properties.setMmsi("413999999");
        properties.getCollect().getPersist().setEnabled(true);
        properties.getCollect().getPersist().setMinWriteIntervalSeconds(0);

        throttle = new PersistenceThrottle(properties);
        dataHandler = new ModbusDataHandler(persistenceService, properties, throttle);
        modbusParser = new ModbusParser(dataHandler);
        spiedParser = spy(modbusParser);

        collector = new SocketPollingCollector(spiedParser);
        collector.setUnitId(1);
        collector.setStartAddress(0);
        collector.setQuantity(16);
    }

    /**
     * 构建包含 16 个寄存器的合法 Modbus TCP 0x03 响应帧
     */
    private byte[] buildValid0x03Response(int txId, int unitId) {
        int byteCount = 32; // 16 registers * 2
        int length = 1 + 1 + 1 + byteCount; // unitId(1) + fc(1) + byteCount(1) + data(32) = 35 (0x0023)
        byte[] frame = new byte[6 + length];

        // MBAP Header
        frame[0] = (byte) ((txId >> 8) & 0xFF);
        frame[1] = (byte) (txId & 0xFF);
        frame[2] = 0x00; // Protocol ID high
        frame[3] = 0x00; // Protocol ID low
        frame[4] = (byte) ((length >> 8) & 0xFF);
        frame[5] = (byte) (length & 0xFF);

        // Unit ID & PDU
        frame[6] = (byte) (unitId & 0xFF);
        frame[7] = 0x03; // Function Code: Read Holding Registers
        frame[8] = (byte) byteCount;

        // 16 寄存器数值示例（rpm=12000 即 1200.0 rpm, temp=850 即 85.0 度等）
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
    private byte[] buildExceptionResponse(int txId, int unitId, int exceptionCode) {
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
    @DisplayName("Test 1: 正常 0x03 响应打通 Modbus TCP -> Parser -> Handler -> Persistence 全链路")
    void testValid0x03ResponseFlowsThroughParserToPersistence() throws Exception {
        int expectedTxId = collector.getTransactionCount();
        byte[] validFrame = buildValid0x03Response(expectedTxId, 1);

        ByteArrayInputStream in = new ByteArrayInputStream(validFrame);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // 1. 执行网络轮询与 MBAP 事务校验
        ModbusTcpResponse response = collector.pollOnce(in, out);
        assertNotNull(response);
        assertFalse(response.isException());
        assertEquals(expectedTxId, response.transactionId());
        assertEquals(1, response.unitId());
        assertEquals(0x03, response.functionCode());
        assertEquals(16, response.registers().length);

        // 2. 执行响应处理分发
        collector.processResponse(response);

        // 3. 断言 ModbusParser 被调用一次，且参数精准无误
        verify(spiedParser, times(1)).parse(eq(1), argThat(pdu -> pdu != null && (pdu[0] & 0xFF) == 0x03));

        // 4. 断言最终打通现有持久化服务 saveEngine
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
    @DisplayName("Test 2: Transaction ID 不匹配时抛出异常，绝不进入 ModbusParser 与持久化层")
    void testTransactionIdMismatchThrowsAndNeverInvokesParserOrPersistence() {
        int expectedTxId = collector.getTransactionCount();
        // 构造对端返回错误的 txId (如 999 != expectedTxId)
        byte[] badFrame = buildValid0x03Response(999, 1);

        ByteArrayInputStream in = new ByteArrayInputStream(badFrame);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // 严格断言抛出 ModbusProtocolException
        assertThrows(ModbusProtocolException.class, () -> collector.pollOnce(in, out));

        // 严格断言 Parser 与持久化从未被触发
        verify(spiedParser, never()).parse(anyInt(), any());
        verify(persistenceService, never()).saveEngine(
                anyString(), anyInt(), anyDouble(), anyDouble(), anyDouble(), anyDouble(),
                anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyInt(), anyInt(), anyInt(), anyInt()
        );
    }

    @Test
    @DisplayName("Test 3: Modbus 异常响应 (0x83) 严禁调用 ModbusParser 与进入业务持久化")
    void testModbusExceptionResponseBlockedFromParserAndPersistence() throws Exception {
        int expectedTxId = collector.getTransactionCount();
        byte[] exceptionFrame = buildExceptionResponse(expectedTxId, 1, 0x02);

        ByteArrayInputStream in = new ByteArrayInputStream(exceptionFrame);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ModbusTcpResponse response = collector.pollOnce(in, out);
        assertNotNull(response);
        assertTrue(response.isException(), "响应必须被标记为异常");
        assertEquals(0x02, response.exceptionCode());

        // 触发处理
        collector.processResponse(response);

        // 严格断言：异常响应被 Collector 拦截，绝对不调用 ModbusParser，不调用持久化
        verify(spiedParser, never()).parse(anyInt(), any());
        verify(persistenceService, never()).saveEngine(
                anyString(), anyInt(), anyDouble(), anyDouble(), anyDouble(), anyDouble(),
                anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyInt(), anyInt(), anyInt(), anyInt()
        );
    }

    @Test
    @DisplayName("Test 4: 重复合法轮询确保每个合法 response 只处理一次")
    void testRepeatedPollingProcessesEachResponseExactlyOnce() throws Exception {
        int tx1 = collector.getTransactionCount();
        byte[] frame1 = buildValid0x03Response(tx1, 1);

        int tx2 = tx1 + 1;
        byte[] frame2 = buildValid0x03Response(tx2, 1);

        // 第一轮轮询
        ByteArrayInputStream in1 = new ByteArrayInputStream(frame1);
        ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        ModbusTcpResponse r1 = collector.pollOnce(in1, out1);
        collector.processResponse(r1);

        assertEquals(1, collector.getTransactionCount() - tx1);
        verify(spiedParser, times(1)).parse(eq(1), any());
        verify(persistenceService, times(1)).saveEngine(
                anyString(), anyInt(), anyDouble(), anyDouble(), anyDouble(), anyDouble(),
                anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyInt(), anyInt(), anyInt(), anyInt()
        );

        // 第二轮轮询
        ByteArrayInputStream in2 = new ByteArrayInputStream(frame2);
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        ModbusTcpResponse r2 = collector.pollOnce(in2, out2);
        collector.processResponse(r2);

        assertEquals(2, collector.getTransactionCount() - tx1);
        // 验证累计刚好各调用 2 次，每个合法响应精准消费一次，无重复消费
        verify(spiedParser, times(2)).parse(eq(1), any());
        verify(persistenceService, times(2)).saveEngine(
                anyString(), anyInt(), anyDouble(), anyDouble(), anyDouble(), anyDouble(),
                anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyInt(), anyInt(), anyInt(), anyInt()
        );
    }
}
