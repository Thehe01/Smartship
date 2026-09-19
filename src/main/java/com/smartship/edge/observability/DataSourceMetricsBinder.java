package com.smartship.edge.observability;

import com.smartship.edge.routing.PoolSnapshot;
import com.smartship.edge.routing.ShipDataSourceManager;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 动态分船 HikariCP 连接池全局聚合 Gauge 绑定器
 * <p>
 * 通过 ShipDataSourceManager.listPoolSnapshots() 计算全局聚合指标，
 * 避免对每个 MMSI 动态生成独立 Label 产生高基数（High Cardinality）问题。
 */
@Component
@RequiredArgsConstructor
public class DataSourceMetricsBinder implements MeterBinder {

    private final ShipDataSourceManager shipDataSourceManager;

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("smartship_datasource_pool_count", this, DataSourceMetricsBinder::getPoolCount)
                .description("Total number of dynamic HikariCP pools")
                .register(registry);

        Gauge.builder("smartship_datasource_connections_total", this, DataSourceMetricsBinder::getTotalConnections)
                .description("Total connections across all dynamic HikariCP pools")
                .register(registry);

        Gauge.builder("smartship_datasource_connections_active", this, DataSourceMetricsBinder::getActiveConnections)
                .description("Active connections across all dynamic HikariCP pools")
                .register(registry);

        Gauge.builder("smartship_datasource_connections_idle", this, DataSourceMetricsBinder::getIdleConnections)
                .description("Idle connections across all dynamic HikariCP pools")
                .register(registry);

        Gauge.builder("smartship_datasource_threads_pending", this, DataSourceMetricsBinder::getPendingThreads)
                .description("Threads awaiting connection across all dynamic HikariCP pools")
                .register(registry);
    }

    public int getPoolCount() {
        return getSnapshots().size();
    }

    public int getTotalConnections() {
        return getSnapshots().stream().mapToInt(PoolSnapshot::getTotalConnections).sum();
    }

    public int getActiveConnections() {
        return getSnapshots().stream().mapToInt(PoolSnapshot::getActiveConnections).sum();
    }

    public int getIdleConnections() {
        return getSnapshots().stream().mapToInt(PoolSnapshot::getIdleConnections).sum();
    }

    public int getPendingThreads() {
        return getSnapshots().stream().mapToInt(PoolSnapshot::getPendingThreads).sum();
    }

    private List<PoolSnapshot> getSnapshots() {
        return shipDataSourceManager.listPoolSnapshots();
    }
}
