package com.smartship.edge.observability;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * 单船单库 HikariCP 连接池 Gauge 绑定器（无分船路由）。
 *
 * <p>船端只有一个本地库：指标直接取自单 {@link DataSource}，不再聚合多池、
 * 不再使用 MMSI 标签，彻底规避高基数问题。
 */
@Component
public class DataSourceMetricsBinder implements MeterBinder {

    private final DataSource dataSource;

    public DataSourceMetricsBinder(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("smartship_datasource_pool_count", this, b -> 1.0)
                .description("Single local HikariCP pool")
                .register(registry);

        Gauge.builder("smartship_datasource_connections_total", this, b -> (double) b.getTotalConnections())
                .description("Total connections of the local HikariCP pool")
                .register(registry);

        Gauge.builder("smartship_datasource_connections_active", this, b -> (double) b.getActiveConnections())
                .description("Active connections of the local HikariCP pool")
                .register(registry);

        Gauge.builder("smartship_datasource_connections_idle", this, b -> (double) b.getIdleConnections())
                .description("Idle connections of the local HikariCP pool")
                .register(registry);

        Gauge.builder("smartship_datasource_threads_pending", this, b -> (double) b.getPendingThreads())
                .description("Threads awaiting connection of the local HikariCP pool")
                .register(registry);
    }

    public int getPoolCount() {
        return 1;
    }

    public int getTotalConnections() {
        return poolStat(Bean::getTotalConnections);
    }

    public int getActiveConnections() {
        return poolStat(Bean::getActiveConnections);
    }

    public int getIdleConnections() {
        return poolStat(Bean::getIdleConnections);
    }

    public int getPendingThreads() {
        return poolStat(Bean::getThreadsAwaitingConnection);
    }

    private interface Bean {
        int getTotalConnections();
        int getActiveConnections();
        int getIdleConnections();
        int getThreadsAwaitingConnection();
    }

    private int poolStat(java.util.function.ToIntFunction<Bean> f) {
        try {
            if (dataSource instanceof HikariDataSource hikari && !hikari.isClosed()
                    && hikari.getHikariPoolMXBean() != null) {
                var mx = hikari.getHikariPoolMXBean();
                Bean b = new Bean() {
                    public int getTotalConnections() { return mx.getTotalConnections(); }
                    public int getActiveConnections() { return mx.getActiveConnections(); }
                    public int getIdleConnections() { return mx.getIdleConnections(); }
                    public int getThreadsAwaitingConnection() { return mx.getThreadsAwaitingConnection(); }
                };
                return f.applyAsInt(b);
            }
        } catch (Exception ignored) {
            // 池关闭/过渡期降级为 0
        }
        return 0;
    }
}
