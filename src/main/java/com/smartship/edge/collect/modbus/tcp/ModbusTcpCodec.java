package com.smartship.edge.collect.modbus.tcp;

import lombok.extern.slf4j.Slf4j;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * 工业级 Modbus TCP MBAP 编解码器与流式分包引擎
 * <p>
 * 报文结构规范：
 * 1. MBAP 报文头（7 字节）：
 *    - 事务标识符（Transaction Identifier）: 2 字节，大端序，用于主从请求/响应匹配
 *    - 协议标识符（Protocol Identifier）: 2 字节，Modbus 协议恒为 0x0000
 *    - 长度（Length）: 2 字节，大端序，表示后续字节数（即 1 字节单元标识符 + PDU 长度）
 *    - 单元标识符（Unit Identifier）: 1 字节，从站地址 (0~255)
 * 2. 协议数据单元（PDU）：
 *    - 功能码（Function Code）: 1 字节
 *    - 数据区（Data）: 包含地址、寄存器数、字节计数或寄存器具体数值
 * <p>
 * 流控与抗粘包机制：
 * 依托 MBAP Header 中的 Length 字段作为定长契约，先读取 7 字节头部，再精准抓取 Length - 1 字节 PDU，
 * 彻底消除以太网底层 MTU 切片引发的 TCP 半包（Half-packet）与粘包（Sticky-packet）故障。
 */
@Slf4j
public class ModbusTcpCodec {

    public static final int MBAP_HEADER_LENGTH = 7;
    public static final int STANDARD_READ_REQUEST_LENGTH = 12;
    public static final int MAX_FRAME_LENGTH = 260; // MBAP(7) + PDU(253)

    private ModbusTcpCodec() {
    }

    /**
     * 编码 Modbus TCP 请求报文（含标准 7 字节 MBAP 头与 5 字节读指令 PDU，共 12 字节）
     */
    public static byte[] encodeRequest(ModbusTcpRequest req) {
        if (req == null) {
            throw new IllegalArgumentException("ModbusTcpRequest 不能为空");
        }

        byte[] frame = new byte[STANDARD_READ_REQUEST_LENGTH];
        // 1. Transaction ID (2 字节)
        frame[0] = (byte) ((req.transactionId() >> 8) & 0xFF);
        frame[1] = (byte) (req.transactionId() & 0xFF);

        // 2. Protocol ID (2 字节, 恒为 0)
        frame[2] = (byte) ((req.protocolId() >> 8) & 0xFF);
        frame[3] = (byte) (req.protocolId() & 0xFF);

        // 3. Length (2 字节, 后续包含 UnitId 1字节 + FunctionCode 1字节 + StartAddr 2字节 + Quantity 2字节 = 6 字节)
        frame[4] = 0x00;
        frame[5] = 0x06;

        // 4. Unit ID / Slave ID (1 字节)
        frame[6] = (byte) (req.unitId() & 0xFF);

        // 5. Function Code (1 字节)
        frame[7] = (byte) (req.functionCode() & 0xFF);

        // 6. Start Address (2 字节)
        frame[8] = (byte) ((req.startAddress() >> 8) & 0xFF);
        frame[9] = (byte) (req.startAddress() & 0xFF);

        // 7. Quantity of Registers (2 字节)
        frame[10] = (byte) ((req.quantity() >> 8) & 0xFF);
        frame[11] = (byte) (req.quantity() & 0xFF);

        return frame;
    }

    /**
     * 解码 Modbus TCP 响应报文
     */
    public static ModbusTcpResponse decodeResponse(byte[] frame) {
        if (frame == null || frame.length < MBAP_HEADER_LENGTH + 1) {
            throw new ModbusProtocolException("Modbus TCP 响应帧长度不足: " + (frame == null ? 0 : frame.length));
        }

        int txId = ((frame[0] & 0xFF) << 8) | (frame[1] & 0xFF);
        int protocolId = ((frame[2] & 0xFF) << 8) | (frame[3] & 0xFF);
        if (protocolId != 0) {
            throw new ModbusProtocolException("非法的 Modbus TCP 协议标识符 (期望 0): " + protocolId);
        }

        int length = ((frame[4] & 0xFF) << 8) | (frame[5] & 0xFF);
        int expectedTotalLength = 6 + length;
        if (frame.length < expectedTotalLength) {
            throw new ModbusProtocolException("报文长度不足: MBAP 声明总长度 " + expectedTotalLength + " 字节，实际仅 " + frame.length + " 字节");
        }

        int unitId = frame[6] & 0xFF;
        int funcCode = frame[7] & 0xFF;
        boolean isException = (funcCode & 0x80) != 0;
        int exceptionCode = 0;
        int[] registers = new int[0];

        int pduLength = length - 1;
        byte[] pdu = new byte[pduLength];
        System.arraycopy(frame, 7, pdu, 0, pduLength);

        if (isException) {
            if (pduLength >= 2) {
                exceptionCode = frame[8] & 0xFF;
            }
        } else if (funcCode == ModbusTcpRequest.FC_READ_HOLDING_REGISTERS || funcCode == ModbusTcpRequest.FC_READ_INPUT_REGISTERS) {
            if (frame.length < 9) {
                throw new ModbusProtocolException("读寄存器响应缺少字节计数字段");
            }
            int byteCount = frame[8] & 0xFF;
            int regCount = byteCount / 2;
            registers = new int[regCount];
            for (int i = 0; i < regCount; i++) {
                int regOffset = 9 + i * 2;
                if (regOffset + 1 < frame.length) {
                    registers[i] = ((frame[regOffset] & 0xFF) << 8) | (frame[regOffset + 1] & 0xFF);
                }
            }
        }

        return new ModbusTcpResponse(txId, protocolId, unitId, funcCode, isException, exceptionCode, pdu, registers);
    }

    /**
     * 从 TCP Socket 输入流中精准读取一帧完整的 Modbus TCP 报文
     * <p>
     * 核心设计：
     * 1. 阻塞抓取前 7 字节 MBAP Header
     * 2. 提取并校验 Length 字段
     * 3. 阻塞抓取剩余 Length - 1 字节 PDU
     * 4. 遇到对端平滑断开返回 null；遇到流式分包通过循环 readFully 拼装补齐，防止半包粘包
     *
     * @param in 输入流
     * @return 完整的 Modbus TCP 数据帧字节数组，或在流关闭时返回 null
     * @throws IOException             网络 I/O 异常
     * @throws ModbusProtocolException 协议长度或格式异常
     */
    public static byte[] readFrame(InputStream in) throws IOException {
        byte[] header = new byte[MBAP_HEADER_LENGTH];
        try {
            readFully(in, header, 0, MBAP_HEADER_LENGTH);
        } catch (EOFException e) {
            return null; // 对端安全关闭
        }

        int length = ((header[4] & 0xFF) << 8) | (header[5] & 0xFF);
        // Length 包含 UnitId (1B) + PDU (至少功能码 1B) = 最少 2 字节
        if (length < 2 || length > (MAX_FRAME_LENGTH - 6)) {
            throw new ModbusProtocolException("非法的 MBAP Length 字段值: " + length);
        }

        int remainingPduLength = length - 1;
        byte[] fullFrame = new byte[6 + length];
        System.arraycopy(header, 0, fullFrame, 0, MBAP_HEADER_LENGTH);

        readFully(in, fullFrame, MBAP_HEADER_LENGTH, remainingPduLength);
        return fullFrame;
    }

    /**
     * 确保从输入流中读满指定长度字节，防止 TCP 半包导致的截断
     */
    public static void readFully(InputStream in, byte[] buffer, int offset, int length) throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int read = in.read(buffer, offset + totalRead, length - totalRead);
            if (read == -1) {
                if (totalRead == 0) {
                    throw new EOFException("对端已关闭输入流");
                }
                throw new EOFException("读取未完成数据帧，期望 " + length + " 字节，但在读取 " + totalRead + " 字节后对端关闭");
            }
            totalRead += read;
        }
    }
}
