package com.smartship.edge.collector.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class ModbusUtils {
    private static final Logger log = LoggerFactory.getLogger(ModbusUtils.class);

    private ModbusUtils() {
    }

    /**
     * 构建 Modbus 读寄存器请求帧（含 CRC 校验）
     */
    public static byte[] buildReadRequest(int slaveId, int startReg, int regCount, int functionCode) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(slaveId);
        output.write((byte) functionCode);
        output.write((startReg >> 8) & 0xFF);
        output.write(startReg & 0xFF);
        output.write((regCount >> 8) & 0xFF);
        output.write(regCount & 0xFF);
        byte[] crc = calculateCRC(output.toByteArray());
        output.write(crc[0]);
        output.write(crc[1]);
        return output.toByteArray();
    }

    /**
     * 解析响应报文中的寄存器数值（大端序）
     */
    public static List<Integer> parseResponse(byte[] response) {
        List<Integer> values = new ArrayList<>();
        if (response == null || response.length < 3) {
            return values;
        }
        int byteCount = response[2] & 0xFF;
        for (int i = 0; i < byteCount && (4 + i < response.length); i += 2) {
            int high = (response[3 + i] & 0xFF) << 8;
            int low = response[4 + i] & 0xFF;
            values.add(high | low);
        }
        return values;
    }

    /**
     * 计算 Modbus CRC16
     */
    public static byte[] calculateCRC(byte[] data) {
        int crc = 0xFFFF;
        for (byte b : data) {
            crc ^= b & 0xFF;
            for (int i = 0; i < 8; ++i) {
                if ((crc & 1) != 0) {
                    crc >>= 1;
                    crc ^= 0xA001;
                } else {
                    crc >>= 1;
                }
            }
        }
        return new byte[]{(byte) (crc & 0xFF), (byte) ((crc >> 8) & 0xFF)};
    }
}
