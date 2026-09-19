package com.smartship.edge.collect.modbus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Modbus TCP 数据解析器
 * <p>
 * 遵循工业 Modbus 标准：大端序高低字节拼装，按 Slave ID 将寄存器分发至主机、发电机及舵机业务流
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ModbusParser {

    private final ModbusDataHandler dataHandler;

    public void parse(int slaveId, byte[] pdu) {
        if (pdu == null || pdu.length < 2) {
            log.warn("[ModbusParser] PDU 数据长度不足");
            return;
        }

        int funcCode = pdu[0] & 0xFF;
        int byteCount = pdu[1] & 0xFF;
        int quantity = byteCount / 2;

        if (funcCode == 0x03 || funcCode == 0x04) {
            // 读保持寄存器 / 输入寄存器响应
            int[] registers = new int[quantity];
            for (int i = 0; i < quantity; i++) {
                int idx = 2 + i * 2;
                if (idx + 1 < pdu.length) {
                    // 大端序（Big-Endian）：高字节在前，低字节在后
                    registers[i] = ((pdu[idx] & 0xFF) << 8) | (pdu[idx + 1] & 0xFF);
                }
            }

            String deviceType = dataHandler.getDeviceType(slaveId);
            log.debug("[ModbusParser] 从站: {}, 设备类型: {}, 寄存器数: {}", slaveId, deviceType, quantity);

            switch (deviceType) {
                case "engine" -> dataHandler.handleEngineData(registers, slaveId);
                case "motor" -> dataHandler.handleMotorData(registers, slaveId);
                case "steering" -> dataHandler.handleSteeringData(registers, slaveId);
                default -> dataHandler.handleEngineData(registers, slaveId);
            }
        } else if (funcCode > 0x80) {
            log.error("[ModbusParser] 收到异常响应: 功能码 0x{}, 异常码: {}",
                    Integer.toHexString(funcCode), pdu[1] & 0xFF);
        } else {
            log.warn("[ModbusParser] 未知功能码: 0x{}", Integer.toHexString(funcCode));
        }
    }
}
