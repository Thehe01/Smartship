package com.smartship.edge.routing;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.config.MmsiPersistence;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

/**
 * 船舶自注册与动态建表服务（即插即用）
 * <p>
 * 当从 AIS !AIVDO 中识别到合法的本船 MMSI 后，自动向目标分船库刷入 schema/ship-schema.sql 时序表结构
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShipAutoRegisterService {

    private final ShipDataSourceManager shipDataSourceManager;
    private final EdgeProperties properties;
    private String preparedMmsi;

    public synchronized void ensureRegistered(String mmsi) {
        if (mmsi == null || mmsi.isEmpty()) {
            properties.setSchemaReady(false);
            return;
        }
        if (mmsi.equals(preparedMmsi) && properties.isSchemaReady()) {
            return;
        }

        ShipDataSourceManager.ShipDatabase registry = shipDataSourceManager.resolveRegistry(null, mmsi);
        if (registry == null || !registry.enabled()) {
            properties.setSchemaReady(false);
            preparedMmsi = null;
            log.warn("[AutoRegister] 船舶 MMSI: {} 未在注册表中配置或已被禁用，跳过入库准备", mmsi);
            return;
        }

        properties.setMmsi(registry.mmsi());
        properties.setShipId(registry.shipId());
        try {
            JdbcTemplate jt = shipDataSourceManager.getJdbcTemplate(null, registry.mmsi());
            executeSerialDataSchema(jt);
            properties.setSchemaReady(true);
            preparedMmsi = registry.mmsi();
            MmsiPersistence.write(registry.mmsi());
            log.info("[AutoRegister] 成功就绪船舶库: mmsi={}, shipId={}, db={}",
                    registry.mmsi(), registry.shipId(), registry.databaseName());
        } catch (Exception e) {
            properties.setSchemaReady(false);
            preparedMmsi = null;
            log.warn("[AutoRegister] 初始化船舶库时序表失败: mmsi={}, err={}", registry.mmsi(), e.getMessage());
        }
    }

    private void executeSerialDataSchema(JdbcTemplate jt) {
        try {
            ClassPathResource resource = new ClassPathResource("schema/ship-schema.sql");
            if (!resource.exists()) {
                throw new IllegalStateException("缺少 schema/ship-schema.sql，无法初始化船舶独立时序库结构");
            }
            String sql = new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            String cleaned = sql.replaceAll("(?m)^--.*$", "");
            for (String statement : cleaned.split(";")) {
                String trimmed = statement.trim();
                if (!trimmed.isEmpty()) {
                    jt.execute(trimmed);
                }
            }
            log.debug("[AutoRegister] 目标船库时序表结构检查/自建完成");
        } catch (Exception e) {
            throw new IllegalStateException("时序表结构初始化异常: " + e.getMessage(), e);
        }
    }
}
