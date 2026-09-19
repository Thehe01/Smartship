package com.smartship.edge.routing;

import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单船动态数据源上下文实体
 * <p>
 * 封装单艘船舶的元数据配置、底层 HikariDataSource、JdbcTemplate、配置指纹、创建时间戳以及最近校验时间戳，
 * 支持原子幂等的连接池安全关闭与轻量 TTL 校验。
 */
@Slf4j
@Getter
public class ShipDataSourceContext {

    private final ShipDataSourceManager.ShipDatabase registry;
    private final HikariDataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final String configFingerprint;
    private final long createdAt;
    private final AtomicLong lastValidatedAt;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ShipDataSourceContext(ShipDataSourceManager.ShipDatabase registry,
                                 HikariDataSource dataSource,
                                 JdbcTemplate jdbcTemplate,
                                 String configFingerprint,
                                 long createdAt,
                                 long lastValidatedAt) {
        this.registry = registry;
        this.dataSource = dataSource;
        this.jdbcTemplate = jdbcTemplate;
        this.configFingerprint = configFingerprint;
        this.createdAt = createdAt;
        this.lastValidatedAt = new AtomicLong(lastValidatedAt);
    }

    public ShipDataSourceContext(ShipDataSourceManager.ShipDatabase registry,
                                 HikariDataSource dataSource,
                                 JdbcTemplate jdbcTemplate,
                                 String configFingerprint,
                                 long createdAt) {
        this(registry, dataSource, jdbcTemplate, configFingerprint, createdAt, createdAt);
    }

    /**
     * 获取最近一次向注册中心校验的时间戳 (毫秒)
     */
    public long getLastValidatedAt() {
        return lastValidatedAt.get();
    }

    /**
     * 标记更新最近校验通过的时间戳
     *
     * @param now 当前时间戳 (毫秒)
     */
    public void markValidated(long now) {
        lastValidatedAt.set(now);
    }

    /**
     * 判断当前数据源上下文是否已被关闭
     */
    public boolean isClosed() {
        return closed.get() || dataSource == null || dataSource.isClosed();
    }

    /**
     * 幂等安全关闭底层 HikariDataSource
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (dataSource != null && !dataSource.isClosed()) {
                try {
                    log.info("[ShipDB] 正在安全优雅关闭连接池: pool={}, mmsi={}",
                            dataSource.getPoolName(), registry != null ? registry.mmsi() : "unknown");
                    dataSource.close();
                } catch (Exception e) {
                    log.warn("[ShipDB] 关闭连接池异常: pool={}, err={}",
                            dataSource.getPoolName(), e.getMessage());
                }
            }
        }
    }
}
