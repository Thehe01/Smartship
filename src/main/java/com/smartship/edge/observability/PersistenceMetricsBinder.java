package com.smartship.edge.observability;

import com.smartship.edge.routing.pool.PersistencePoolMetrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 异步持久化线程池可观测性 Gauge 绑定器
 * <p>
 * 将现有 PersistencePoolMetrics 状态及 CallerRuns 反压降级计数暴露为 Micrometer Gauge，
 * 保证 Gauge 始终读取实时值，且不改变底层线程池运行行为。
 */
@Component
@RequiredArgsConstructor
public class PersistenceMetricsBinder implements MeterBinder {

    private final PersistencePoolMetrics poolMetrics;

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("smartship_persistence_pool_active_threads", poolMetrics, PersistencePoolMetrics::getActiveCount)
                .description("Number of active threads currently executing in persistence pool")
                .register(registry);

        Gauge.builder("smartship_persistence_pool_size", poolMetrics, PersistencePoolMetrics::getPoolSize)
                .description("Current number of worker threads in persistence pool")
                .register(registry);

        Gauge.builder("smartship_persistence_pool_queue_size", poolMetrics, PersistencePoolMetrics::getQueueSize)
                .description("Number of tasks waiting in persistence pool bounded queue")
                .register(registry);

        Gauge.builder("smartship_persistence_pool_queue_remaining", poolMetrics, PersistencePoolMetrics::getQueueRemainingCapacity)
                .description("Remaining capacity of persistence pool bounded queue")
                .register(registry);

        Gauge.builder("smartship_persistence_pool_completed_tasks", poolMetrics, PersistencePoolMetrics::getCompletedTaskCount)
                .description("Total completed task count of persistence pool executor")
                .register(registry);

        Gauge.builder("smartship_persistence_pool_rejections_total", poolMetrics, PersistencePoolMetrics::getRejectCount)
                .description("Total number of tasks rejected by queue capacity and handled by CallerRuns policy")
                .register(registry);
    }
}
