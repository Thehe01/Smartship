package com.smartship.edge.routing;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.exception.RegistryQueryException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分船数据库动态路由与连接池生命周期管理器 (P1-2 Dynamic HikariCP Lifecycle Governance)
 * <p>
 * 核心架构特性：
 * 1. 库级数据隔离（Schema-Level Isolation）：以 MMSI 为缓存 Key 动态路由至每艘船专属的 HikariDataSource 连接池
 * 2. 完备生命周期治理：保留真实 HikariDataSource 实例，支持显式刷新、驱逐与优雅停机销毁
 * 3. 确定性配置指纹（Config Fingerprint）：基于配置计算 SHA-256 摘要，动态比对以决定是否触发重建，不打印明文密码
 * 4. 故障安全（Fail-Safe）切换：严格遵循 create new pool -> replace context -> close old pool 拓扑序，新池失败时原池不受损
 * 5. 并发安全保证：多线程并发首次访问同一 MMSI 时基于细粒度锁保证有且仅有单个连接池存活，避免资源泄漏
 * 6. 快路径无锁读取：高频 getJdbcTemplate 路径 lock-free，低频配置变更操作轻量同步
 * 7. 严格准入与驱逐：未注册或已被禁用（enabled=0）船舶坚决拦截并主动驱逐已有连接池
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShipDataSourceManager {

    private final JdbcTemplate authJdbcTemplate;
    private final EdgeProperties properties;

    /**
     * 以 MMSI 为主键维护的分船动态数据源上下文缓存
     */
    private final Map<String, ShipDataSourceContext> contexts = new ConcurrentHashMap<>();

    /**
     * 辅助索引映射：shipId -> mmsi
     */
    private final Map<String, String> shipIdToMmsi = new ConcurrentHashMap<>();

    /**
     * 分 MMSI 细粒度同步锁，确保并发初次创建与刷新的线程安全性
     */
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    /**
     * 生命周期停机标记，防止停机期间或停机后并发请求导致连接池泄漏
     */
    private volatile boolean destroyed = false;

    private void checkNotDestroyed() {
        if (this.destroyed) {
            throw new IllegalStateException("[ShipDB] ShipDataSourceManager 已经关闭/销毁");
        }
    }

    /**
     * 获取指定船舶对应的 JdbcTemplate（高频快路径无锁，低频未命中走线程安全初始化）
     *
     * @param shipId 船舶业务 ID（可选）
     * @param mmsi   船舶 MMSI（可选，但与 shipId 不能同时为空）
     * @return 绑定到专属 Hikari 连接池的 JdbcTemplate
     */
    public JdbcTemplate getJdbcTemplate(String shipId, String mmsi) {
        String cleanShipId = StringUtils.hasText(shipId) ? shipId.trim() : null;
        String cleanMmsi = StringUtils.hasText(mmsi) ? mmsi.trim() : null;
        if (cleanShipId == null && cleanMmsi == null) {
            throw new IllegalArgumentException("shipId 或 mmsi 不能同时为空");
        }
        checkNotDestroyed();

        // 快路径（Fast Path）：完全无锁，以规范化 MMSI 检索 ConcurrentHashMap
        String knownMmsi = cleanMmsi != null ? cleanMmsi : shipIdToMmsi.get(cleanShipId);
        if (knownMmsi != null) {
            ShipDataSourceContext context = contexts.get(knownMmsi);
            if (context != null && !context.isClosed()) {
                return context.getJdbcTemplate();
            }
        }

        // 慢路径（Slow Path）：首次加载或连接池已失效，进入同步初始化流程
        return getOrCreateContext(cleanShipId, cleanMmsi).getJdbcTemplate();
    }

    /**
     * 便捷重载：仅根据 MMSI 获取 JdbcTemplate
     */
    public JdbcTemplate getJdbcTemplate(String mmsi) {
        return getJdbcTemplate(null, mmsi);
    }

    /**
     * 线程安全获取或创建数据源上下文实体
     */
    private ShipDataSourceContext getOrCreateContext(String shipId, String mmsi) {
        checkNotDestroyed();
        String knownMmsi = mmsi != null ? mmsi : (shipId != null ? shipIdToMmsi.get(shipId) : null);
        if (knownMmsi != null) {
            ShipDataSourceContext existing = contexts.get(knownMmsi);
            if (existing != null && !existing.isClosed()) {
                return existing;
            }
        }

        String targetMmsi = knownMmsi;
        ShipDatabase registry = null;
        if (targetMmsi == null) {
            registry = resolveRegistry(shipId, mmsi);
            if (registry == null || !registry.enabled()) {
                throw new IllegalStateException("[ShipDB] 船舶未在注册表中配置或已被禁用: shipId=" + shipId + ", mmsi=" + mmsi);
            }
            targetMmsi = registry.mmsi();
        }

        Object lock = locks.computeIfAbsent(targetMmsi, k -> new Object());
        synchronized (lock) {
            checkNotDestroyed();
            ShipDataSourceContext existing = contexts.get(targetMmsi);
            if (existing != null && !existing.isClosed()) {
                if (registry != null && StringUtils.hasText(registry.shipId())) {
                    shipIdToMmsi.put(registry.shipId(), targetMmsi);
                }
                return existing;
            }

            if (registry == null) {
                registry = resolveRegistry(shipId, targetMmsi);
                if (registry == null || !registry.enabled()) {
                    throw new IllegalStateException("[ShipDB] 船舶未在注册表中配置或已被禁用: shipId=" + shipId + ", mmsi=" + targetMmsi);
                }
            }

            String canonicalMmsi = registry.mmsi();
            String fingerprint = computeFingerprint(registry);
            HikariDataSource ds = createDataSource(registry);
            JdbcTemplate jt = new JdbcTemplate(ds);
            ShipDataSourceContext newCtx = new ShipDataSourceContext(
                    registry, ds, jt, fingerprint, System.currentTimeMillis()
            );

            if (this.destroyed) {
                newCtx.close();
                throw new IllegalStateException("[ShipDB] ShipDataSourceManager 已经关闭/销毁，拒绝创建新连接池");
            }

            contexts.put(canonicalMmsi, newCtx);
            if (StringUtils.hasText(registry.shipId())) {
                shipIdToMmsi.put(registry.shipId(), canonicalMmsi);
            }
            return newCtx;
        }
    }

    /**
     * 显式刷新指定船舶的数据源配置
     * <p>
     * 核心逻辑：
     * 1. 检查注册信息，若不存在或已被禁用，主动驱逐并关闭已有连接池
     * 2. 计算新指纹，若指纹未变且连接池活跃，跳过并复用现有连接池
     * 3. 若配置发生变化，严格遵循：create new pool -> replace context -> close old pool
     * 4. 若创建新 pool 失败，抛出异常并保证原有旧 pool 完好不受损
     *
     * @param mmsi 船舶 MMSI
     * @return true 表示成功刷新并切换为新连接池；false 表示指纹未变复用原池或已被驱逐
     */
    public boolean refreshDataSource(String mmsi) {
        String cleanMmsi = StringUtils.hasText(mmsi) ? mmsi.trim() : null;
        if (cleanMmsi == null) {
            throw new IllegalArgumentException("mmsi 不能为空");
        }
        checkNotDestroyed();

        Object lock = locks.computeIfAbsent(cleanMmsi, k -> new Object());
        synchronized (lock) {
            checkNotDestroyed();
            ShipDatabase registry = resolveRegistry(null, cleanMmsi);
            if (registry == null) {
                log.warn("[ShipDB] 船舶在注册表中不存在，执行驱逐: mmsi={}", cleanMmsi);
                invalidateDataSource(cleanMmsi);
                return false;
            }

            if (!registry.enabled()) {
                log.warn("[ShipDB] 船舶已被禁用 (enabled=false)，主动驱逐并关闭已有连接池: mmsi={}", cleanMmsi);
                invalidateDataSource(cleanMmsi);
                return false;
            }

            String canonicalMmsi = registry.mmsi();
            String newFingerprint = computeFingerprint(registry);
            ShipDataSourceContext currentContext = contexts.get(canonicalMmsi);

            // 指纹未变且原池仍存活时，直接复用
            if (currentContext != null && !currentContext.isClosed()
                    && Objects.equals(newFingerprint, currentContext.getConfigFingerprint())) {
                log.info("[ShipDB] 船舶 MMSI: {} 配置指纹未发生变化 ({})，复用现有连接池", canonicalMmsi, newFingerprint);
                return false;
            }

            log.info("[ShipDB] 船舶 MMSI: {} 配置指纹发生变化或连接池未激活，开始重建连接池...", canonicalMmsi);

            // 1. 创建新连接池（若此处抛出异常，旧 pool 不会被关闭，上下文不会被污染）
            HikariDataSource newDataSource = createDataSource(registry);
            JdbcTemplate newJdbcTemplate = new JdbcTemplate(newDataSource);
            ShipDataSourceContext newContext = new ShipDataSourceContext(
                    registry,
                    newDataSource,
                    newJdbcTemplate,
                    newFingerprint,
                    System.currentTimeMillis()
            );

            if (this.destroyed) {
                newContext.close();
                throw new IllegalStateException("[ShipDB] ShipDataSourceManager 已经关闭/销毁，拒绝刷新连接池");
            }

            // 2. 替换上下文
            ShipDataSourceContext oldContext = contexts.put(canonicalMmsi, newContext);
            if (StringUtils.hasText(registry.shipId())) {
                shipIdToMmsi.put(registry.shipId(), canonicalMmsi);
            }
            // 若旧上下文登记过不同的 shipId，清理旧 shipId 映射
            if (oldContext != null && oldContext.getRegistry() != null
                    && StringUtils.hasText(oldContext.getRegistry().shipId())
                    && !oldContext.getRegistry().shipId().equals(registry.shipId())) {
                shipIdToMmsi.remove(oldContext.getRegistry().shipId());
            }

            // 3. 安全关闭旧连接池
            if (oldContext != null) {
                log.info("[ShipDB] 船舶 MMSI: {} 新连接池挂载完毕，开始安全关闭旧连接池", canonicalMmsi);
                oldContext.close();
            }

            return true;
        }
    }

    /**
     * 显式主动失效并关闭指定船舶的连接池（生命周期操作保证幂等）
     *
     * @param mmsi 船舶 MMSI
     * @return true 表示成功驱逐并关闭；false 表示原本不存在已缓存连接池
     */
    public boolean invalidateDataSource(String mmsi) {
        String cleanMmsi = StringUtils.hasText(mmsi) ? mmsi.trim() : null;
        if (cleanMmsi == null) {
            return false;
        }

        Object lock = locks.computeIfAbsent(cleanMmsi, k -> new Object());
        synchronized (lock) {
            ShipDataSourceContext context = contexts.remove(cleanMmsi);
            // 清理所有指向该 mmsi 的反向索引映射
            shipIdToMmsi.values().removeIf(m -> m.equals(cleanMmsi));
            if (context != null) {
                context.close();
                log.info("[ShipDB] 成功驱逐并关闭船舶 MMSI: {} 的连接池", cleanMmsi);
                return true;
            }
            return false;
        }
    }

    /**
     * 获取指定 MMSI 的连接池运行时指标快照
     *
     * @param mmsi 船舶 MMSI
     * @return 连接池快照，若不存在则返回 null
     */
    public PoolSnapshot getPoolSnapshot(String mmsi) {
        String cleanMmsi = StringUtils.hasText(mmsi) ? mmsi.trim() : null;
        if (cleanMmsi == null) {
            return null;
        }
        ShipDataSourceContext context = contexts.get(cleanMmsi);
        return context != null ? PoolSnapshot.of(context) : null;
    }

    /**
     * 列出当前所有已加载分船连接池的运行时快照
     */
    public List<PoolSnapshot> listPoolSnapshots() {
        List<PoolSnapshot> snapshots = new ArrayList<>(contexts.size());
        for (ShipDataSourceContext ctx : contexts.values()) {
            PoolSnapshot snapshot = PoolSnapshot.of(ctx);
            if (snapshot != null) {
                snapshots.add(snapshot);
            }
        }
        return Collections.unmodifiableList(snapshots);
    }

    /**
     * 获取指定 MMSI 的数据源治理上下文（供治理探测与单元测试验证）
     */
    public ShipDataSourceContext getDataSourceContext(String mmsi) {
        String cleanMmsi = StringUtils.hasText(mmsi) ? mmsi.trim() : null;
        if (cleanMmsi == null) {
            return null;
        }
        return contexts.get(cleanMmsi);
    }

    /**
     * 容器关闭或退出时，统一安全关闭全部动态分船连接池（幂等）
     */
    @PreDestroy
    public synchronized void closeAll() {
        log.info("[ShipDB] 正在统一关闭全部动态分船连接池 (当前活动池数: {})...", contexts.size());
        this.destroyed = true;

        // 对每一个锁进行同步以等待当前正在执行中的初始化或刷新退出
        for (Object lock : locks.values()) {
            synchronized (lock) {
                // 等待在途临界区结束
            }
        }

        for (Map.Entry<String, ShipDataSourceContext> entry : contexts.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                log.warn("[ShipDB] 关闭连接池异常 (mmsi={}): {}", entry.getKey(), e.getMessage());
            }
        }
        contexts.clear();
        shipIdToMmsi.clear();
        locks.clear();
        log.info("[ShipDB] 全部动态分船连接池已清理完毕");
    }

    /**
     * 计算数据源配置的 SHA-256 确定性指纹
     * <p>
     * 严格覆盖：host, port, databaseName, username, password, maximumPoolSize, jdbcParams
     * 采用 SHA-256 哈希，杜绝明文打印密码。
     */
    public String computeFingerprint(ShipDatabase registry) {
        if (registry == null) {
            return "";
        }
        int maxPoolSize = Math.max(1, properties.getDatasource().getShip().getMaximumPoolSize());
        String jdbcParams = firstText(properties.getDatasource().getShip().getJdbcParams());
        String canonical = "host=" + (registry.host() == null ? "" : registry.host().trim())
                + "|port=" + registry.port()
                + "|db=" + (registry.databaseName() == null ? "" : registry.databaseName().trim())
                + "|user=" + (registry.username() == null ? "" : registry.username().trim())
                + "|pwd=" + (registry.password() == null ? "" : registry.password().trim())
                + "|maxPool=" + maxPoolSize
                + "|params=" + jdbcParams.trim();
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 算法不可用", e);
        }
    }

    /**
     * 从主认证库解析船舶配置信息
     * <p>
     * 异常语义明确区分：
     * - 查无记录：返回 null（正常业务语义）
     * - 数据库/网络查询异常：抛出 RegistryQueryException，杜绝误判为“船舶不存在”并误删正常池
     */
    public ShipDatabase resolveRegistry(String shipId, String mmsi) {
        String cleanShipId = StringUtils.hasText(shipId) ? shipId.trim() : null;
        String cleanMmsi = StringUtils.hasText(mmsi) ? mmsi.trim() : null;
        if (cleanShipId == null && cleanMmsi == null) {
            throw new IllegalArgumentException("shipId 或 mmsi 不能同时为空");
        }

        String where = cleanMmsi != null ? "mmsi = ?" : "ship_id = ?";
        Object arg = cleanMmsi != null ? cleanMmsi : cleanShipId;
        try {
            return authJdbcTemplate.query("""
                            SELECT ship_id, mmsi, database_name, host, port, username, password, enabled
                            FROM ship_database_registry
                            WHERE %s
                            LIMIT 1
                            """.formatted(where),
                    rs -> rs.next()
                            ? new ShipDatabase(
                            rs.getString("ship_id") != null ? rs.getString("ship_id").trim() : null,
                            rs.getString("mmsi") != null ? rs.getString("mmsi").trim() : null,
                            rs.getString("database_name") != null ? rs.getString("database_name").trim() : null,
                            firstText(rs.getString("host"), properties.getDatasource().getShip().getDefaultHost()),
                            rs.getObject("port") == null ? properties.getDatasource().getShip().getDefaultPort() : rs.getInt("port"),
                            firstText(rs.getString("username"), properties.getDatasource().getShip().getDefaultUsername()),
                            firstText(rs.getString("password"), properties.getDatasource().getShip().getDefaultPassword()),
                            rs.getBoolean("enabled"))
                            : null,
                    arg);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (DataAccessException e) {
            log.error("[ShipDB] 查询主认证数据库异常: shipId={}, mmsi={}, error={}", cleanShipId, cleanMmsi, e.getMessage());
            throw new RegistryQueryException("Failed to query ship database registry: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("[ShipDB] 查询船舶注册表未知异常: shipId={}, mmsi={}, error={}", cleanShipId, cleanMmsi, e.getMessage());
            throw new RegistryQueryException("Unexpected error querying ship database registry: " + e.getMessage(), e);
        }
    }

    /**
     * 查询所有已启用的船舶注册配置
     */
    public List<ShipDatabase> listEnabledRegistries() {
        try {
            return authJdbcTemplate.query("""
                            SELECT ship_id, mmsi, database_name, host, port, username, password, enabled
                            FROM ship_database_registry
                            WHERE enabled = 1
                            ORDER BY id
                            """,
                    (rs, rowNum) -> new ShipDatabase(
                            rs.getString("ship_id") != null ? rs.getString("ship_id").trim() : null,
                            rs.getString("mmsi") != null ? rs.getString("mmsi").trim() : null,
                            rs.getString("database_name") != null ? rs.getString("database_name").trim() : null,
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

    /**
     * 构建 HikariDataSource 实例
     */
    protected HikariDataSource createDataSource(ShipDatabase registry) {
        HikariConfig config = new HikariConfig();
        config.setPoolName("ship-db-" + registry.mmsi());
        String jdbcUrl = buildJdbcUrl(registry);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(registry.username());
        config.setPassword(registry.password());

        String customDriver = properties.getDatasource().getShip().getDriverClassName();
        if (StringUtils.hasText(customDriver)) {
            config.setDriverClassName(customDriver);
        } else if (jdbcUrl.startsWith("jdbc:h2:")) {
            config.setDriverClassName("org.h2.Driver");
        } else {
            config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        }

        config.setMaximumPoolSize(Math.max(1, properties.getDatasource().getShip().getMaximumPoolSize()));
        config.setMinimumIdle(1);
        config.setConnectionTimeout(3000);
        log.info("[ShipDB] 为船舶 MMSI: {} 成功构建独立连接池: {}, poolName: {}",
                registry.mmsi(), registry.databaseName(), config.getPoolName());
        return new HikariDataSource(config);
    }

    /**
     * 构建 JDBC URL
     */
    protected String buildJdbcUrl(ShipDatabase registry) {
        if (registry.host() != null && registry.host().startsWith("jdbc:")) {
            return registry.host();
        }
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
        @Override
        public String toString() {
            return "ShipDatabase[" +
                    "shipId=" + shipId +
                    ", mmsi=" + mmsi +
                    ", databaseName=" + databaseName +
                    ", host=" + host +
                    ", port=" + port +
                    ", username=" + username +
                    ", password=******" +
                    ", enabled=" + enabled +
                    ']';
        }
    }
}
