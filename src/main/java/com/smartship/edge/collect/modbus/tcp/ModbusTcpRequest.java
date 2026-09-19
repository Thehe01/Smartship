package com.smartship.edge.collect.modbus.tcp;

/**
 * Modbus TCP 请求对象 (MBAP + PDU)
 *
 * @param transactionId 事务标识符 (2 字节, 用于请求响应匹配)
 * @param protocolId    协议标识符 (2 字节, Modbus 协议恒为 0)
 * @param unitId        单元/从站标识符 (1 字节, 0~255)
 * @param functionCode  功能码 (1 字节, 如 0x03 读保持寄存器, 0x04 读输入寄存器)
 * @param startAddress  起始寄存器地址 (2 字节)
 * @param quantity      寄存器数量 (2 字节)
 */
public record ModbusTcpRequest(
        int transactionId,
        int protocolId,
        int unitId,
        int functionCode,
        int startAddress,
        int quantity
) {
    public static final int PROTOCOL_MODBUS = 0;
    public static final int FC_READ_HOLDING_REGISTERS = 0x03;
    public static final int FC_READ_INPUT_REGISTERS = 0x04;

    public static ModbusTcpRequest readHoldingRegisters(int transactionId, int unitId, int startAddress, int quantity) {
        return new ModbusTcpRequest(transactionId, PROTOCOL_MODBUS, unitId, FC_READ_HOLDING_REGISTERS, startAddress, quantity);
    }

    public static ModbusTcpRequest readInputRegisters(int transactionId, int unitId, int startAddress, int quantity) {
        return new ModbusTcpRequest(transactionId, PROTOCOL_MODBUS, unitId, FC_READ_INPUT_REGISTERS, startAddress, quantity);
    }
}
