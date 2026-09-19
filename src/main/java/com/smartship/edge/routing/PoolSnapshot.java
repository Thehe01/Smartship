package com.smartship.edge.routing;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

/**
 * 分船 HikariCP 连接池运行时状态只读快照
 */
public record PoolSnapshot(
        String poolName,
        int totalConnections,
        int activeConnections,
        int idleConnections,
        int pendingThreads,
        boolean closed,
        long createdAt
) {

    public String getPoolName() {
        return poolName;
    }

    public int getTotalConnections() {
        return totalConnections;
    }

    public int getActiveConnections() {
        return activeConnections;
    }

    public int getIdleConnections() {
        return idleConnections;
    }

    public int getPendingThreads() {
        return pendingThreads;
    }

    public boolean isClosed() {
        return closed;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public static PoolSnapshot of(ShipDataSourceContext context) {
        if (context == null) {
            return null;
        }
        HikariDataSource ds = context.getDataSource();
        String poolName = (ds != null) ? ds.getPoolName() : "";
        boolean closed = context.isClosed();
        int active = 0;
        int idle = 0;
        int total = 0;
        int pending = 0;

        if (!closed && ds != null) {
            try {
                HikariPoolMXBean mxBean = ds.getHikariPoolMXBean();
                if (mxBean != null) {
                    active = mxBean.getActiveConnections();
                    idle = mxBean.getIdleConnections();
                    total = mxBean.getTotalConnections();
                    pending = mxBean.getThreadsAwaitingConnection();
                }
            } catch (Exception ignored) {
                // 连接池在关闭/过渡期间可能抛出异常，安全捕获降级
            }
        }

        return new PoolSnapshot(poolName, total, active, idle, pending, closed, context.getCreatedAt());
    }

    @Override
    public String toString() {
        return String.format(
                "PoolSnapshot[pool=%s, total=%d, active=%d, idle=%d, pending=%d, closed=%b, createdAt=%d]",
                poolName, totalConnections, activeConnections, idleConnections, pendingThreads, closed, createdAt
        );
    }
}
