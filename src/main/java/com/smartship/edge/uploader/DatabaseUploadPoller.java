package com.smartship.edge.uploader;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.ShipDataSourceManager;
import com.smartship.edge.uploader.mqtt.MqttPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 边端数据库增量轮询上传引擎
 * <p>
 * 核心架构特性：
 * 1. 本地游标驱动：以 zncb_upload_cursor 记录每张表的最后推送断点 (last_uploaded_id / last_uploaded_time)
 * 2. 失败短路保护：一旦单条消息发送失败（如断网或弱网），坚决执行 break 暂停批次，冻结游标在断点处，恢复后自动续查
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DatabaseUploadPoller {

    public record IncrementalStream(String tableName, String type, String topicSuffix) {}
    public record SnapshotStream(String tableName, String type, String topicSuffix, String orderColumn) {}

    private static final List<IncrementalStream> TELEMETRY_STREAMS = List.of(
            new IncrementalStream("zncb_gps_data", "nmea_gps", "nmea_gps"),
            new IncrementalStream("zncb_wind_data", "nmea_wind", "nmea_wind"),
            new IncrementalStream("zncb_depth_data", "nmea_depth", "nmea_depth"),
            new IncrementalStream("zncb_rudder_data", "nmea_rudder", "nmea_rudder"),
            new IncrementalStream("zncb_engine_data", "engine", "engine")
    );

    private final EdgeProperties properties;
    private final ShipDataSourceManager shipDataSourceManager;
    private final MqttPublisher mqttPublisher;

    @Scheduled(
            fixedDelayString = "${smartship.edge.uploader.poll.fixed-delay-ms:15000}",
            initialDelayString = "${smartship.edge.uploader.poll.initial-delay-ms:5000}"
    )
    public void scheduledUpload() {
        if (!properties.getUploader().isEnabled()) {
            return;
        }

        for (ShipDataSourceManager.ShipDatabase ship : shipDataSourceManager.listEnabledRegistries()) {
            try {
                JdbcTemplate jdbcTemplate = shipDataSourceManager.getJdbcTemplate(ship.shipId(), ship.mmsi());
                ensureCursorTable(jdbcTemplate);
                for (IncrementalStream stream : TELEMETRY_STREAMS) {
                    uploadIncrementalStream(jdbcTemplate, ship, stream);
                }
            } catch (Exception e) {
                log.warn("[Uploader] 轮询推送异常: shipId={}, mmsi={}, err={}",
                        ship.shipId(), ship.mmsi(), e.getMessage());
            }
        }
    }

    private void uploadIncrementalStream(JdbcTemplate jdbcTemplate,
                                         ShipDataSourceManager.ShipDatabase ship,
                                         IncrementalStream stream) {
        try {
            long lastId = getOrCreateCursorId(jdbcTemplate, stream.tableName());
            int limit = Math.max(1, properties.getUploader().getPoll().getBatchSize());

            List<Map<String, Object>> rows = jdbcTemplate.query(
                    "SELECT * FROM " + stream.tableName() + " WHERE id > ? ORDER BY id ASC LIMIT ?",
                    (rs, rowNum) -> toMap(rs),
                    lastId, limit
            );

            if (rows.isEmpty()) {
                return;
            }

            long maxSuccessId = lastId;
            int successCount = 0;

            for (Map<String, Object> row : rows) {
                Object idObj = row.get("id");
                long rowId = (idObj instanceof Number num) ? num.longValue() : Long.parseLong(String.valueOf(idObj));
                prepareRow(ship, row);

                boolean ok = mqttPublisher.publish(ship.mmsi(), stream.type(), stream.topicSuffix(), row);
                if (ok) {
                    maxSuccessId = Math.max(maxSuccessId, rowId);
                    successCount++;
                } else {
                    // 弱网断网保护：立即 break 跳出，绝不继续发送，完整保留断点！
                    log.warn("[Uploader] MQTT 发送失败，短路中断当前批次以冻结续传断点: table={}, failId={}",
                            stream.tableName(), rowId);
                    break;
                }
            }

            // 仅在成功时推进游标
            if (maxSuccessId > lastId) {
                updateCursorId(jdbcTemplate, stream.tableName(), maxSuccessId);
                log.info("[Uploader] 上传成功: shipId={}, table={}, count={}, 游标推进: {} -> {}",
                        ship.shipId(), stream.tableName(), successCount, lastId, maxSuccessId);
            }
        } catch (Exception e) {
            log.warn("[Uploader] 增量表查询/上报异常: table={}, err={}", stream.tableName(), e.getMessage());
        }
    }

    private void prepareRow(ShipDataSourceManager.ShipDatabase ship, Map<String, Object> row) {
        row.putIfAbsent("ship_id", ship.shipId());
        row.putIfAbsent("mmsi", ship.mmsi());
        row.putIfAbsent("source_database", ship.databaseName());
    }

    private void ensureCursorTable(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS zncb_upload_cursor (
                stream_name VARCHAR(64) NOT NULL,
                partition_key VARCHAR(64) NOT NULL DEFAULT '',
                last_uploaded_id BIGINT NOT NULL DEFAULT 0,
                last_uploaded_time DATETIME NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (stream_name, partition_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """);
    }

    private long getOrCreateCursorId(JdbcTemplate jdbcTemplate, String tableName) {
        List<Long> results = jdbcTemplate.query(
                "SELECT last_uploaded_id FROM zncb_upload_cursor WHERE stream_name = ? AND partition_key = ''",
                (rs, rowNum) -> rs.getLong(1),
                tableName
        );
        if (!results.isEmpty()) {
            return results.get(0);
        }
        long initialId = 0L;
        jdbcTemplate.update(
                "INSERT INTO zncb_upload_cursor (stream_name, partition_key, last_uploaded_id, updated_at) VALUES (?, '', ?, NOW())",
                tableName, initialId
        );
        return initialId;
    }

    private void updateCursorId(JdbcTemplate jdbcTemplate, String tableName, long lastId) {
        jdbcTemplate.update(
                "UPDATE zncb_upload_cursor SET last_uploaded_id = ?, updated_at = NOW() WHERE stream_name = ? AND partition_key = ''",
                lastId, tableName
        );
    }

    private Map<String, Object> toMap(ResultSet rs) {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int count = meta.getColumnCount();
            Map<String, Object> map = new LinkedHashMap<>(count);
            for (int i = 1; i <= count; i++) {
                String label = meta.getColumnLabel(i);
                Object val = rs.getObject(i);
                if (val instanceof Timestamp ts) {
                    val = ts.toLocalDateTime();
                }
                map.put(label.toLowerCase(), val);
            }
            return map;
        } catch (Exception e) {
            throw new IllegalStateException("ResultSet 转换 Map 失败: " + e.getMessage(), e);
        }
    }
}
