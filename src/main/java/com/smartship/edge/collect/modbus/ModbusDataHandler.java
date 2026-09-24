package com.smartship.edge.collect.modbus;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PersistenceThrottle;
import com.smartship.edge.routing.service.EnginePoint;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import com.smartship.edge.routing.service.WriteBatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

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

    /**
     * 主机工况攒批器（批量开关开启时启用，按船分组、满批刷出）。
     * 延迟创建：默认关闭时保持 null，行为与旧单行路径完全一致。
     */
    private volatile WriteBatcher<String, EnginePoint> engineBatcher;
    private volatile int engineBatcherSize = -1;

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
                if (properties.getCollect().getPersist().isEngineBatchEnabled()) {
                    batchEnginePoint(mmsi, slaveId, rpm, coolantTemp, lubeOilPress,
                            fuelPress, exhaustTemp, tcAirPress, startAirPress,
                            bearingTemp, batteryVolt, runningHours, status,
                            alarmBits1, alarmBits2);
                } else {
                    persistenceService.saveEngine(
                            mmsi, slaveId, rpm, coolantTemp, lubeOilPress,
                            fuelPress, exhaustTemp, tcAirPress, startAirPress,
                            bearingTemp, batteryVolt, runningHours, status,
                            alarmBits1, alarmBits2
                    );
                }
            }
        }
    }

    /**
     * 批量分支：同船攒满一批即作为<b>一个</b>异步任务提交（一次 batchUpdate）。
     * 攒批器按配置的 batchSize 延迟创建；配置变更时重建（旧缓存先刷空，不丢行）。
     */
    private void batchEnginePoint(String mmsi, int slaveId,
                                  double rpm, double coolantTemp, double lubeOilPress,
                                  double fuelPress, double exhaustTemp, double tcAirPress,
                                  double startAirPress, double bearingTemp, double batteryVolt,
                                  int runningHours, int status, int alarmBits1, int alarmBits2) {
        EnginePoint point = EnginePoint.now(mmsi, slaveId, rpm, coolantTemp, lubeOilPress,
                fuelPress, exhaustTemp, tcAirPress, startAirPress, bearingTemp,
                batteryVolt, runningHours, status, alarmBits1, alarmBits2);
        batcher().add(mmsi, point, this::submitBatch);
    }

    private void submitBatch(List<EnginePoint> fullBatch) {
        persistenceService.saveEngineBatchAsync(fullBatch);
    }

    private synchronized WriteBatcher<String, EnginePoint> batcher() {
        int size = Math.max(1, properties.getCollect().getPersist().getEngineBatchSize());
        WriteBatcher<String, EnginePoint> b = engineBatcher;
        if (b == null || engineBatcherSize != size) {
            if (b != null) {
                b.drainAll(this::submitBatch);
            }
            b = new WriteBatcher<>(size);
            engineBatcher = b;
            engineBatcherSize = size;
        }
        return b;
    }

    /**
     * 尾批兜底：每秒刷出未满批的行（稀疏船舶不满批也不会饿死）。
     * 开关关闭或攒批器尚未创建时为空操作；单测直接构造 handler 时该方法不会被调度触发。
     */
    @Scheduled(fixedDelay = 1000)
    public void flushEngineBatches() {
        if (!properties.getCollect().getPersist().isEngineBatchEnabled()) {
            return;
        }
        WriteBatcher<String, EnginePoint> b = engineBatcher;
        if (b != null) {
            b.drainAll(this::submitBatch);
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
