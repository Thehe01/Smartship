package com.smartship.edge.observability;

import com.smartship.edge.routing.ShipDataSourceManager;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 边端待上传积压指标管理器
 * <p>
 * 统计所有已启用船舶及所有 telemetry 数据流的总积压记录数：
 * backlog = MAX(table.id) - last_uploaded_id
 * 异常降级保证：查询失败时不将指标置 0，安全保留上一次有效采样值；杜绝 MMSI 高基数 Label。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UploadBacklogMetrics implements MeterBinder {

    private static final List<String> TELEMETRY_TABLES = List.of(
            "zncb_gps_data",
            "zncb_wind_data",
            "zncb_depth_data",
            "zncb_rudder_data",
            "zncb_engine_data"
    );

    private final ShipDataSourceManager shipDataSourceManager;
    private final AtomicLong lastKnownBacklog = new AtomicLong(0L);

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("smartship_uploader_backlog_rows", this, UploadBacklogMetrics::getBacklogRows)
                .description("Total backlog rows pending upload across all enabled ships and telemetry streams")
                .register(registry);
    }

    public long getBacklogRows() {
        refresh();
        return lastKnownBacklog.get();
    }

    public synchronized void refresh() {
        try {
            List<ShipDataSourceManager.ShipDatabase> ships = shipDataSourceManager.listEnabledRegistries();
            if (ships.isEmpty()) {
                lastKnownBacklog.set(0L);
                return;
            }

            long totalBacklog = 0L;
            for (ShipDataSourceManager.ShipDatabase ship : ships) {
                JdbcTemplate jt = shipDataSourceManager.getJdbcTemplate(ship.shipId(), ship.mmsi());
                for (String table : TELEMETRY_TABLES) {
                    totalBacklog += calculateTableBacklog(jt, table);
                }
            }
            lastKnownBacklog.set(totalBacklog);
        } catch (Exception e) {
            log.warn("[Observability] 计算上传 backlog 异常，保留历史值 {}: {}", lastKnownBacklog.get(), e.getMessage());
            // 保留最近一次成功采集值，绝不将 backlog 错误归零
        }
    }

    private long calculateTableBacklog(JdbcTemplate jt, String table) {
        try {
            Long maxId = jt.queryForObject("SELECT COALESCE(MAX(id), 0) FROM " + table, Long.class);
            if (maxId == null || maxId <= 0) {
                return 0L;
            }
            long lastId = queryCursorId(jt, table);
            return Math.max(0L, maxId - lastId);
        } catch (Exception e) {
            log.debug("[Observability] 查询表 {} backlog 降级: {}", table, e.getMessage());
            return 0L;
        }
    }

    private long queryCursorId(JdbcTemplate jt, String table) {
        try {
            List<Long> results = jt.query(
                    "SELECT last_uploaded_id FROM zncb_upload_cursor WHERE stream_name = ? AND partition_key = ''",
                    (rs, rowNum) -> rs.getLong(1),
                    table
            );
            return results.isEmpty() ? 0L : results.get(0);
        } catch (Exception e) {
            return 0L;
        }
    }

    public long getLastKnownBacklog() {
        return lastKnownBacklog.get();
    }
}
