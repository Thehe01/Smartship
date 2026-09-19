package com.smartship.edge.routing;

import com.smartship.edge.config.EdgeProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分船数据库动态路由与连接池管理器
 * <p>
 * 核心架构特性：
 * 1. 库级数据隔离（Schema-Level Isolation）：以 MMSI 动态路由至每艘船的专属数据库
 * 2. 线程安全懒加载：基于 ConcurrentHashMap.computeIfAbsent 在首次访问时动态初始化 HikariDataSource
 * 3. 严格准入拦截：对未注册或已禁用（enabled=0）的 MMSI 坚决拦截抛出异常，杜绝不同船舶数据串库
 * 4. 边缘连接数限额：每艘船连接池最大连接数严格约束为 5，防止边缘工控机连接数爆炸
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShipDataSourceManager {

    private final JdbcTemplate authJdbcTemplate;
    private final EdgeProperties properties;
    private final Map<String, JdbcTemplate> shipJdbcTemplates = new ConcurrentHashMap<>();

    public JdbcTemplate getJdbcTemplate(String shipId, String mmsi) {
        ShipDatabase registry = resolveRegistry(shipId, mmsi);
        if (registry == null || !registry.enabled()) {
            throw new IllegalStateException("[ShipDB] 船舶未在注册表中配置或已被禁用: shipId=" + shipId + ", mmsi=" + mmsi);
        }
        return shipJdbcTemplates.computeIfAbsent(registry.databaseName(),
                key -> new JdbcTemplate(createDataSource(registry)));
    }

    public ShipDatabase resolveRegistry(String shipId, String mmsi) {
        if (!StringUtils.hasText(shipId) && !StringUtils.hasText(mmsi)) {
            throw new IllegalArgumentException("shipId 或 mmsi 不能同时为空");
        }

        String where = StringUtils.hasText(shipId) ? "ship_id = ?" : "mmsi = ?";
        Object arg = StringUtils.hasText(shipId) ? shipId : mmsi;
        try {
            return authJdbcTemplate.query("""
                            SELECT ship_id, mmsi, database_name, host, port, username, password, enabled
                            FROM ship_database_registry
                            WHERE %s
                            LIMIT 1
                            """.formatted(where),
                    rs -> rs.next()
                            ? new ShipDatabase(
                            rs.getString("ship_id"),
                            rs.getString("mmsi"),
                            rs.getString("database_name"),
                            firstText(rs.getString("host"), properties.getDatasource().getShip().getDefaultHost()),
                            rs.getObject("port") == null ? properties.getDatasource().getShip().getDefaultPort() : rs.getInt("port"),
                            firstText(rs.getString("username"), properties.getDatasource().getShip().getDefaultUsername()),
                            firstText(rs.getString("password"), properties.getDatasource().getShip().getDefaultPassword()),
                            rs.getBoolean("enabled"))
                            : null,
                    arg);
        } catch (Exception e) {
            log.warn("[ShipDB] 查询船舶注册表失败: {}", e.getMessage());
            return null;
        }
    }

    public List<ShipDatabase> listEnabledRegistries() {
        try {
            return authJdbcTemplate.query("""
                            SELECT ship_id, mmsi, database_name, host, port, username, password, enabled
                            FROM ship_database_registry
                            WHERE enabled = 1
                            ORDER BY id
                            """,
                    (rs, rowNum) -> new ShipDatabase(
                            rs.getString("ship_id"),
                            rs.getString("mmsi"),
                            rs.getString("database_name"),
                            firstText(rs.getString("host"), properties.getDatasource().getShip().getDefaultHost()),
                            rs.getObject("port") == null ? properties.getDatasource().getShip().getDefaultPort() : rs.getInt("port"),
                            firstText(rs.getString("username"), properties.getDatasource().getShip().getDefaultUsername()),
                            firstText(rs.getString("password"), properties.getDatasource().getShip().getDefaultPassword()),
                            rs.getBoolean("enabled")));
        } catch (Exception e) {
            log.warn("[ShipDB] 列出可用船舶失败: {}", e.getMessage());
            return List.of();
        }
    }

    private HikariDataSource createDataSource(ShipDatabase registry) {
        HikariConfig config = new HikariConfig();
        config.setPoolName("ship-db-" + registry.mmsi());
        config.setJdbcUrl(buildJdbcUrl(registry));
        config.setUsername(registry.username());
        config.setPassword(registry.password());
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(Math.max(1, properties.getDatasource().getShip().getMaximumPoolSize()));
        config.setMinimumIdle(1);
        config.setConnectionTimeout(3000);
        log.info("[ShipDB] 为船舶 MMSI: {} 成功构建独立连接池: {}", registry.mmsi(), registry.databaseName());
        return new HikariDataSource(config);
    }

    private String buildJdbcUrl(ShipDatabase registry) {
        String params = firstText(properties.getDatasource().getShip().getJdbcParams());
        return "jdbc:mysql://" + registry.host() + ":" + registry.port() + "/" + registry.databaseName()
                + (StringUtils.hasText(params) ? "?" + params : "");
    }

    private String firstText(String... values) {
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return value;
            }
        }
        return "";
    }

    public record ShipDatabase(String shipId, String mmsi, String databaseName, String host, int port,
                               String username, String password, boolean enabled) {
    }
}
