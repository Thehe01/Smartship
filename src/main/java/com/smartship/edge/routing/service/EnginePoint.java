package com.smartship.edge.routing.service;

import java.time.LocalDateTime;

/**
 * 单个主机工况测量点（批量写入的最小单元）。
 * <p>
 * 与 {@code NmeaDataPersistenceService.saveEngine} 的单行参数一一对应，
 * {@code timestamp} 在构造时采集，避免批量攒批期间时间失真。
 */
public record EnginePoint(
        String mmsi,
        int slaveId,
        double rpm,
        double coolantTemp,
        double lubeOilPress,
        double fuelPress,
        double exhaustTemp,
        double tcAirPress,
        double startAirPress,
        double bearingTemp,
        double batteryVolt,
        int runningHours,
        int status,
        int alarmBits1,
        int alarmBits2,
        LocalDateTime timestamp) {

    public EnginePoint {
        if (mmsi == null || mmsi.isEmpty()) {
            throw new IllegalArgumentException("mmsi must not be empty");
        }
        if (timestamp == null) {
            throw new IllegalArgumentException("timestamp must not be null");
        }
    }

    /** 采集时刻为 now 的便捷构造。 */
    public static EnginePoint now(String mmsi, int slaveId,
                                  double rpm, double coolantTemp, double lubeOilPress,
                                  double fuelPress, double exhaustTemp, double tcAirPress,
                                  double startAirPress, double bearingTemp, double batteryVolt,
                                  int runningHours, int status, int alarmBits1, int alarmBits2) {
        return new EnginePoint(mmsi, slaveId, rpm, coolantTemp, lubeOilPress,
                fuelPress, exhaustTemp, tcAirPress, startAirPress, bearingTemp,
                batteryVolt, runningHours, status, alarmBits1, alarmBits2,
                LocalDateTime.now());
    }
}
