package com.smartship.edge.routing.pool;

import com.smartship.edge.config.EdgeProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 边缘端异步持久化专用线程池配置
 * <p>
 * 核心架构特性：
 * 1. 资源严格有界：核心线程 2，最大线程 4，有界阻塞队列 500，杜绝工控机内存 OOM；
 * 2. 命名工控线程：persistence-worker-{counter}，便于 jstack 与 APM 追踪；
 * 3. 智能降级反压：装配 MonitoredCallerRunsPolicy，满载时由调用者线程同步入库，并自动累计拒绝指标；
 * 4. 优雅停机保护：等待 30 秒确保内存队列中已收敛的航行数据完整落库；
 * 5. 独立专用命名：专供 @Async("persistenceExecutor") 使用，不接管全局默认异步调度。
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class PersistenceAsyncConfig {

    private final EdgeProperties properties;

    @Bean
    public MonitoredCallerRunsPolicy persistenceRejectionPolicy() {
        return new MonitoredCallerRunsPolicy();
    }

    @Bean(name = "persistenceExecutor")
    public ThreadPoolTaskExecutor persistenceExecutor(MonitoredCallerRunsPolicy rejectionPolicy) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        EdgeProperties.PoolConfig pool = properties.getCollect().getPersist().getPool();

        int coreSize = pool != null ? pool.getCoreSize() : 2;
        int maxSize = pool != null ? pool.getMaxSize() : 4;
        int queueCapacity = pool != null ? pool.getQueueCapacity() : 500;
        int keepAlive = pool != null ? pool.getKeepAliveSeconds() : 60;
        int awaitSeconds = pool != null ? pool.getAwaitTerminationSeconds() : 30;

        executor.setCorePoolSize(coreSize);
        executor.setMaxPoolSize(maxSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setKeepAliveSeconds(keepAlive);
        executor.setThreadNamePrefix("persistence-worker-");
        executor.setRejectedExecutionHandler(rejectionPolicy);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(awaitSeconds);
        executor.initialize();

        log.info("[PersistencePool] 初始化专用持久化有界线程池: core={}, max={}, queueCapacity={}, awaitTermination={}s",
                coreSize, maxSize, queueCapacity, awaitSeconds);
        return executor;
    }
}
