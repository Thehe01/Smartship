package com.smartship.edge;

import com.smartship.edge.collect.modbus.tcp.ModbusProtocolException;
import com.smartship.edge.collect.modbus.tcp.ModbusTcpCodec;
import com.smartship.edge.collect.modbus.tcp.ModbusTcpRequest;
import com.smartship.edge.collect.modbus.tcp.ModbusTcpResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class ModbusTcpCodecTest {

    @Test
    @DisplayName("测试 Modbus TCP 请求编码（标准 12 字节 MBAP + PDU）")
    void testEncodeRequest() {
        // 请求：事务 ID = 0x0102, 从站 1, 功能码 0x03, 起始地址 0x0004, 寄存器数 2
        ModbusTcpRequest request = ModbusTcpRequest.readHoldingRegisters(0x0102, 1, 4, 2);
        byte[] frame = ModbusTcpCodec.encodeRequest(request);

        assertNotNull(frame);
        assertEquals(12, frame.length, "标准 Modbus TCP 读请求应为 12 字节");

        // 验证 MBAP 报文头
        assertEquals((byte) 0x01, frame[0]); // TxId High
        assertEquals((byte) 0x02, frame[1]); // TxId Low
        assertEquals((byte) 0x00, frame[2]); // Protocol High (0)
        assertEquals((byte) 0x00, frame[3]); // Protocol Low (0)
        assertEquals((byte) 0x00, frame[4]); // Length High
        assertEquals((byte) 0x06, frame[5]); // Length Low (UnitId 1B + PDU 5B = 6)
        assertEquals((byte) 0x01, frame[6]); // UnitId

        // 验证 PDU
        assertEquals((byte) 0x03, frame[7]); // Function Code
        assertEquals((byte) 0x00, frame[8]); // Starting Addr High
        assertEquals((byte) 0x04, frame[9]); // Starting Addr Low
        assertEquals((byte) 0x00, frame[10]); // Quantity High
        assertEquals((byte) 0x02, frame[11]); // Quantity Low
    }

    @Test
    @DisplayName("测试 Modbus TCP 正常响应解码与大端序寄存器还原")
    void testDecodeNormalResponse() {
        // 构造响应帧：TxId=1, ProtId=0, Length=7, UnitId=1, Func=0x03, ByteCount=4, Reg1=1000(0x03E8), Reg2=250(0x00FA)
        byte[] rawResponse = new byte[]{
                0x00, 0x01, // TxId = 1
                0x00, 0x00, // ProtocolId = 0
                0x00, 0x07, // Length = 7 (UnitId 1B + PDU 6B)
                0x01,       // UnitId = 1
                0x03,       // FuncCode = 0x03
                0x04,       // ByteCount = 4
                0x03, (byte) 0xE8, // Reg0 = 1000
                0x00, (byte) 0xFA  // Reg1 = 250
        };

        ModbusTcpResponse response = ModbusTcpCodec.decodeResponse(rawResponse);
        assertNotNull(response);
        assertEquals(1, response.transactionId());
        assertEquals(0, response.protocolId());
        assertEquals(1, response.unitId());
        assertEquals(0x03, response.functionCode());
        assertFalse(response.isException());
        assertEquals(2, response.registers().length);
        assertEquals(1000, response.registers()[0], "大端序高字节 0x03, 低字节 0xE8 应解析为 1000");
        assertEquals(250, response.registers()[1], "大端序高字节 0x00, 低字节 0xFA 应解析为 250");
    }

    @Test
    @DisplayName("测试 Modbus TCP 异常响应解码")
    void testDecodeExceptionResponse() {
        // 异常响应帧：TxId=1, ProtId=0, Length=3, UnitId=1, Func=0x83, ExCode=0x02 (Illegal Data Address)
        byte[] exceptionFrame = new byte[]{
                0x00, 0x01, // TxId
                0x00, 0x00, // ProtId
                0x00, 0x03, // Length = 3 (UnitId 1B + FuncCode 1B + ExCode 1B)
                0x01,       // UnitId
                (byte) 0x83,// FuncCode 0x83
                0x02        // Exception Code: Illegal Data Address
        };

        ModbusTcpResponse response = ModbusTcpCodec.decodeResponse(exceptionFrame);
        assertNotNull(response);
        assertTrue(response.isException(), "功能码 >= 0x80 应识别为异常响应");
        assertEquals(0x83, response.functionCode());
        assertEquals(0x02, response.exceptionCode(), "异常码应为 0x02");
        assertEquals(0, response.registers().length);
    }

    @Test
    @DisplayName("测试非法协议标识符拒绝")
    void testInvalidProtocolId() {
        byte[] corruptFrame = new byte[]{
                0x00, 0x01,
                0x00, 0x01, // 非法协议 ID (非 0)
                0x00, 0x03,
                0x01,
                0x03,
                0x02
        };

        assertThrows(ModbusProtocolException.class, () -> ModbusTcpCodec.decodeResponse(corruptFrame));
    }

    @Test
    @DisplayName("测试流式分包读取与 TCP 半包重组（readFrame）")
    void testStreamingHalfPacketReassembly() throws IOException {
        // 准备一个包含 2 个寄存器的标准响应
        byte[] frameBytes = new byte[]{
                0x00, 0x05, // TxId = 5
                0x00, 0x00, // ProtId = 0
                0x00, 0x07, // Length = 7
                0x01,       // UnitId = 1
                0x03,       // FuncCode = 0x03
                0x04,       // ByteCount = 4
                0x01, 0x02, // Reg0
                0x03, 0x04  // Reg1
        };

        // 使用管道流模拟 TCP 字节流，分 3 次碎片写入以模拟以太网半包
        PipedOutputStream out = new PipedOutputStream();
        PipedInputStream in = new PipedInputStream(out);

        Thread producer = new Thread(() -> {
            try {
                // 碎片 1：写入 4 字节
                out.write(frameBytes, 0, 4);
                out.flush();
                Thread.sleep(10);

                // 碎片 2：写入 5 字节
                out.write(frameBytes, 4, 5);
                out.flush();
                Thread.sleep(10);

                // 碎片 3：写入剩余 4 字节
                out.write(frameBytes, 9, frameBytes.length - 9);
                out.flush();
            } catch (Exception ignored) {
            }
        });
        producer.start();

        byte[] assembled = ModbusTcpCodec.readFrame(in);
        assertNotNull(assembled);
        assertArrayEquals(frameBytes, assembled, "多次分片到达的半包数据应被完整组装");

        ModbusTcpResponse res = ModbusTcpCodec.decodeResponse(assembled);
        assertEquals(5, res.transactionId());
        assertEquals(2, res.registers().length);
        assertEquals(0x0102, res.registers()[0]);
        assertEquals(0x0304, res.registers()[1]);
    }

    @Test
    @DisplayName("测试 Modbus TCP 事务一致性校验成功（TxId / UnitId / FuncCode 匹配）")
    void testValidateResponseSuccess() {
        ModbusTcpRequest request = ModbusTcpRequest.readHoldingRegisters(102, 1, 0, 16);
        ModbusTcpResponse response = new ModbusTcpResponse(
                102, 0, 1, 0x03, false, 0, new byte[0], new int[]{10, 20}
        );

        assertDoesNotThrow(() -> ModbusTcpCodec.validateResponse(request, response));
    }

    @Test
    @DisplayName("测试 Modbus TCP 事务 ID 不匹配时抛出 ModbusProtocolException")
    void testValidateResponseTxMismatch() {
        ModbusTcpRequest request = ModbusTcpRequest.readHoldingRegisters(102, 1, 0, 16);
        ModbusTcpResponse mismatchedTxResponse = new ModbusTcpResponse(
                103, 0, 1, 0x03, false, 0, new byte[0], new int[]{10, 20}
        );

        ModbusProtocolException ex = assertThrows(ModbusProtocolException.class,
                () -> ModbusTcpCodec.validateResponse(request, mismatchedTxResponse));
        assertTrue(ex.getMessage().contains("Transaction ID Mismatch"));
    }

    @Test
    @DisplayName("测试 Modbus TCP 从站单元 ID 不匹配时抛出 ModbusProtocolException")
    void testValidateResponseUnitMismatch() {
        ModbusTcpRequest request = ModbusTcpRequest.readHoldingRegisters(102, 1, 0, 16);
        ModbusTcpResponse mismatchedUnitResponse = new ModbusTcpResponse(
                102, 0, 2, 0x03, false, 0, new byte[0], new int[]{10, 20}
        );

        ModbusProtocolException ex = assertThrows(ModbusProtocolException.class,
                () -> ModbusTcpCodec.validateResponse(request, mismatchedUnitResponse));
        assertTrue(ex.getMessage().contains("Unit ID Mismatch"));
    }
}
