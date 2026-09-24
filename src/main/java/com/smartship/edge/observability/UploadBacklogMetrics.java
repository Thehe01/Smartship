package com.smartship.edge.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 边端待上传积压指标管理器（单船单库）。
 *
 * <p>统计本船各 telemetry 数据流的总积压记录数：
 * backlog = MAX(table.id) - last_uploaded_id
 * 异常降级保证：查询失败时不将指标置 0，安全保留上一次有效采样值。
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

    private final JdbcTemplate jdbcTemplate;
    private final AtomicLong lastKnownBacklog = new AtomicLong(0L);

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("smartship_uploader_backlog_rows", this, UploadBacklogMetrics::getBacklogRows)
                .description("Total backlog rows pending upload for the local ship database")
                .register(registry);
    }

    public long getBacklogRows() {
        return lastKnownBacklog.get();
    }

    public synchronized void refresh() {
        try {
            long sampledBacklog = 0L;
            for (String table : TELEMETRY_TABLES) {
                sampledBacklog += calculateTableBacklog(jdbcTemplate, table);
            }
            lastKnownBacklog.set(sampledBacklog);
        } catch (Exception e) {
            log.warn("[Observability] 计算上传 backlog 异常，保留上一轮 backlog {}: {}", lastKnownBacklog.get(), e.getMessage());
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
        } catch (BadSqlGrammarException e) {
            // 表尚未就绪/不存在，积压视为 0
            log.debug("[Observability] 表 {} 尚未就绪，积压视为 0: {}", table, e.getMessage());
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
        } catch (BadSqlGrammarException e) {
            // 游标表尚未初始化/不存在，视为从未上传（游标为 0）
            return 0L;
        }
    }

    public long getLastKnownBacklog() {
        return lastKnownBacklog.get();
    }
}
