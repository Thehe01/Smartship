package com.smartship.edge.collect.modbus.tcp;

import java.util.Arrays;

/**
 * Modbus TCP 响应对象 (MBAP + PDU 解码结果)
 *
 * @param transactionId 事务标识符
 * @param protocolId    协议标识符
 * @param unitId        单元/从站标识符
 * @param functionCode  功能码
 * @param isException   是否为异常响应 (功能码最高位为 1, 即 >= 0x80)
 * @param exceptionCode 异常码 (仅在 isException 为 true 时有效)
 * @param pdu           原始 PDU 字节数组 (不含 MBAP Header)
 * @param registers     按大端序解析后的 16 位寄存器整数数组
 */
public record ModbusTcpResponse(
        int transactionId,
        int protocolId,
        int unitId,
        int functionCode,
        boolean isException,
        int exceptionCode,
        byte[] pdu,
        int[] registers
) {
    @Override
    public String toString() {
        return "ModbusTcpResponse{" +
                "transactionId=" + transactionId +
                ", unitId=" + unitId +
                ", functionCode=0x" + Integer.toHexString(functionCode) +
                ", isException=" + isException +
                (isException ? ", exceptionCode=" + exceptionCode : ", registers=" + Arrays.toString(registers)) +
                '}';
    }
}
