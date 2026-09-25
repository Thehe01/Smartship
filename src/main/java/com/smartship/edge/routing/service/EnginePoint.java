package com.smartship.edge.routing.service;

import java.time.LocalDateTime;

/**
 * 单个主机工况测量点（批量写入的最小单元）。
 * <p>
 * 与 {@code NmeaDataPersistenceService.saveEngine} 的单行参数一一对应，
 * {@code timestamp} 在构造时采集，避免批量攒批期间时间失真。
 * <p>{@code replayId} 是全生命周期唯一的幂等键：入批时生成一次，batch insert、
 * 逐行 retry、DB fallback、磁盘 spool、回放、上传 msg_id 全程复用，永不重新生成。
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
        LocalDateTime timestamp,
        String replayId) {

    public EnginePoint {
        if (mmsi == null || mmsi.isEmpty()) {
            throw new IllegalArgumentException("mmsi must not be empty");
        }
        if (timestamp == null) {
            throw new IllegalArgumentException("timestamp must not be null");
        }
        if (replayId == null || replayId.isBlank()) {
            throw new IllegalArgumentException("replayId must not be blank");
        }
    }

    /** 采集时刻为 now、幂等键随机生成的便捷构造。 */
    public static EnginePoint now(String mmsi, int slaveId,
                                  double rpm, double coolantTemp, double lubeOilPress,
                                  double fuelPress, double exhaustTemp, double tcAirPress,
                                  double startAirPress, double bearingTemp, double batteryVolt,
                                  int runningHours, int status, int alarmBits1, int alarmBits2) {
        return new EnginePoint(mmsi, slaveId, rpm, coolantTemp, lubeOilPress,
                fuelPress, exhaustTemp, tcAirPress, startAirPress, bearingTemp,
                batteryVolt, runningHours, status, alarmBits1, alarmBits2,
                LocalDateTime.now(), java.util.UUID.randomUUID().toString());
    }
}
