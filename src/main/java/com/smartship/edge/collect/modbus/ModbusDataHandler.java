package com.smartship.edge.collect.modbus;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PersistenceThrottle;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Modbus 动力与电气工程量还原处理器
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ModbusDataHandler {

    private final NmeaDataPersistenceService persistenceService;
    private final EdgeProperties properties;
    private final PersistenceThrottle throttle;

    public String getDeviceType(int slaveId) {
        if (slaveId >= 1 && slaveId <= 2) return "engine";
        if (slaveId >= 3 && slaveId <= 5) return "motor";
        if (slaveId >= 6 && slaveId <= 7) return "steering";
        return "unknown";
    }

    public void handleEngineData(int[] registers, int slaveId) {
        if (registers.length < 13) {
            log.warn("[Modbus] 主机寄存器长度不足: {} < 13", registers.length);
            return;
        }

        // 工程比例因子还原
        double rpm = registers[0] / 10.0;
        double coolantTemp = registers[1] / 10.0;
        double lubeOilPress = registers[2] / 1000.0; // MPa
        double fuelPress = registers[3] / 1000.0;
        double exhaustTemp = registers[4] / 10.0;
        double tcAirPress = registers[5] / 1000.0;
        double startAirPress = registers[6] / 1000.0;
        double bearingTemp = registers[7] / 10.0;
        double batteryVolt = registers[8] / 10.0;
        int runningHours = registers[9];
        int status = registers[10];
        int alarmBits1 = registers[11];
        int alarmBits2 = registers[12];

        String mmsi = properties.getMmsi();
        if (mmsi != null && !mmsi.isEmpty()) {
            if (throttle.shouldWrite(mmsi + ":modbus:engine:" + slaveId)) {
                persistenceService.saveEngine(
                        mmsi, slaveId, rpm, coolantTemp, lubeOilPress,
                        fuelPress, exhaustTemp, tcAirPress, startAirPress,
                        bearingTemp, batteryVolt, runningHours, status,
                        alarmBits1, alarmBits2
                );
            }
        }
    }

    public void handleMotorData(int[] registers, int slaveId) {
        // 电机/发电机类似处理
        log.debug("[Modbus] 电机从站: {} 收到数据", slaveId);
    }

    public void handleSteeringData(int[] registers, int slaveId) {
        // 舵机类似处理
        log.debug("[Modbus] 舵机从站: {} 收到数据", slaveId);
    }
}
