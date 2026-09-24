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
 * 单船本地库初始化服务（无分船路由）。
 *
 * <p>船端只存本船数据：唯一数据源即 Spring 默认单数据源（本地本船库，
 * 表结构见 {@code schema/ship-schema.sql}）。本服务只做两件事：
 * 1) 确保本地时序表结构存在；2) 置位 {@code schemaReady} 并持久化 MMSI。
 * 方法签名刻意与旧 {@code ShipAutoRegisterService#ensureRegistered} 保持一致，
 * 以最小化 {@code NmeaParser} 改动。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShipLocalInitializer {

    private final JdbcTemplate jdbcTemplate;
    private final EdgeProperties properties;
    private volatile String preparedMmsi;

    public synchronized void ensureRegistered(String mmsi) {
        if (mmsi == null || mmsi.isEmpty()) {
            properties.setSchemaReady(false);
            return;
        }
        if (mmsi.equals(preparedMmsi) && properties.isSchemaReady()) {
            return;
        }

        properties.setMmsi(mmsi);
        if (properties.getShipId() == null || properties.getShipId().isEmpty()) {
            properties.setShipId(mmsi);
        }
        try {
            executeShipSchema(jdbcTemplate);
            properties.setSchemaReady(true);
            preparedMmsi = mmsi;
            MmsiPersistence.write(mmsi);
            log.info("[LocalInit] 本船库已就绪: mmsi={}, shipId={}", mmsi, properties.getShipId());
        } catch (Exception e) {
            properties.setSchemaReady(false);
            preparedMmsi = null;
            log.warn("[LocalInit] 初始化本船库时序表失败: mmsi={}, err={}", mmsi, e.getMessage());
        }
    }

    private void executeShipSchema(JdbcTemplate jt) {
        try {
            ClassPathResource resource = new ClassPathResource("schema/ship-schema.sql");
            if (!resource.exists()) {
                throw new IllegalStateException("缺少 schema/ship-schema.sql，无法初始化本船时序库结构");
            }
            String sql = new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            String cleaned = sql.replaceAll("(?m)^--.*$", "");
            for (String statement : cleaned.split(";")) {
                String trimmed = statement.trim();
                if (!trimmed.isEmpty()) {
                    jt.execute(trimmed);
                }
            }
            log.debug("[LocalInit] 本船库时序表结构检查/自建完成");
        } catch (Exception e) {
            throw new IllegalStateException("时序表结构初始化异常: " + e.getMessage(), e);
        }
    }
}
