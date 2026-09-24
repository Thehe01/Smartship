package com.smartship.edge.uploader;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.observability.SmartShipMetrics;
import com.smartship.edge.observability.UploadBacklogMetrics;
import com.smartship.edge.routing.ShipDataSourceManager;
import com.smartship.edge.uploader.mqtt.MqttPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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
 * 3. 统计增量上报 rows_total 与 batches_total 指标，游标语义严格不变
 */
@Slf4j
@Service
public class DatabaseUploadPoller {

    public record IncrementalStream(String streamKey, String tableName, String type, String topicSuffix) {
        public IncrementalStream(String tableName, String type, String topicSuffix) {
            this(inferStreamKey(tableName), tableName, type, topicSuffix);
        }

        private static String inferStreamKey(String tableName) {
            if (tableName.contains("gps")) return "gps";
            if (tableName.contains("wind")) return "wind";
            if (tableName.contains("depth")) return "depth";
            if (tableName.contains("rudder")) return "rudder";
            if (tableName.contains("engine")) return "engine";
            return "unknown";
        }
    }

    public record SnapshotStream(String tableName, String type, String topicSuffix, String orderColumn) {}

    private static final List<IncrementalStream> TELEMETRY_STREAMS = List.of(
            new IncrementalStream("gps", "zncb_gps_data", "nmea_gps", "nmea_gps"),
            new IncrementalStream("wind", "zncb_wind_data", "nmea_wind", "nmea_wind"),
            new IncrementalStream("depth", "zncb_depth_data", "nmea_depth", "nmea_depth"),
            new IncrementalStream("rudder", "zncb_rudder_data", "nmea_rudder", "nmea_rudder"),
            new IncrementalStream("engine", "zncb_engine_data", "engine", "engine")
    );

    private final EdgeProperties properties;
    private final ShipDataSourceManager shipDataSourceManager;
    private final MqttPublisher mqttPublisher;
    private final SmartShipMetrics metrics;
    private final UploadBacklogMetrics backlogMetrics;
    private final UploadAckTracker ackTracker;

    @Autowired
    public DatabaseUploadPoller(EdgeProperties properties,
                                ShipDataSourceManager shipDataSourceManager,
                                MqttPublisher mqttPublisher,
                                SmartShipMetrics metrics,
                                UploadBacklogMetrics backlogMetrics) {
        this(properties, shipDataSourceManager, mqttPublisher, metrics, backlogMetrics,
                new UploadAckTracker());
    }

    public DatabaseUploadPoller(EdgeProperties properties,
                                ShipDataSourceManager shipDataSourceManager,
                                MqttPublisher mqttPublisher,
                                SmartShipMetrics metrics,
                                UploadBacklogMetrics backlogMetrics,
                                UploadAckTracker ackTracker) {
        this.properties = properties;
        this.shipDataSourceManager = shipDataSourceManager;
        this.mqttPublisher = mqttPublisher;
        this.metrics = metrics;
        this.backlogMetrics = backlogMetrics;
        this.ackTracker = ackTracker;
        // 岸端 Application ACK 回来直接进跟踪器（msg_id 全局唯一，无需 topic 定位表）。
        if (this.mqttPublisher.getClientManager() != null) {
            this.mqttPublisher.getClientManager().setAckListener((topic, msgId, seq) ->
                    ackTracker.onAck(extractMmsi(topic), msgId, seq));
        }
    }

    /** 测试观察：直接驱动 ACK 流转，无需真实 Broker。 */
    UploadAckTracker getAckTracker() {
        return ackTracker;
    }

    private static String extractMmsi(String topic) {
        if (topic == null) {
            return "";
        }
        String[] parts = topic.split("/");
        return parts.length >= 2 ? parts[1] : "";
    }

    public DatabaseUploadPoller(EdgeProperties properties,
                                ShipDataSourceManager shipDataSourceManager,
                                MqttPublisher mqttPublisher) {
        this(properties, shipDataSourceManager, mqttPublisher, null, null);
    }

    @Scheduled(
            fixedDelayString = "${smartship.edge.uploader.poll.fixed-delay-ms:15000}",
            initialDelayString = "${smartship.edge.uploader.poll.initial-delay-ms:5000}"
    )
    public void scheduledUpload() {
        if (!properties.getUploader().isEnabled()) {
            return;
        }

        List<ShipDataSourceManager.ShipDatabase> ships;
        try {
            ships = shipDataSourceManager.listEnabledRegistries();
        } catch (Exception e) {
            log.warn("[Uploader] 获取可用船舶列表异常: {}", e.getMessage());
            ships = List.of();
        }

        for (ShipDataSourceManager.ShipDatabase ship : ships) {
            try {
                JdbcTemplate jdbcTemplate = shipDataSourceManager.getJdbcTemplate(ship.shipId(), ship.mmsi());
                ensureCursorTable(jdbcTemplate);
                for (IncrementalStream stream : TELEMETRY_STREAMS) {
                    uploadIncrementalStream(jdbcTemplate, ship, stream);
                }
            } catch (Exception e) {
                if (metrics != null) {
                    for (IncrementalStream stream : TELEMETRY_STREAMS) {
                        metrics.recordUploadBatch(stream.streamKey(), false);
                    }
                }
                log.warn("[Uploader] 轮询推送异常: shipId={}, mmsi={}, err={}",
                        ship.shipId(), ship.mmsi(), e.getMessage());
            }
        }
        if (backlogMetrics != null) {
            backlogMetrics.refresh();
        }
    }

    public void uploadIncrementalStream(JdbcTemplate jdbcTemplate,
                                        ShipDataSourceManager.ShipDatabase ship,
                                        IncrementalStream stream) {
        boolean batchRecorded = false;
        try {
            long lastId = getOrCreateCursorId(jdbcTemplate, stream.tableName());
            int limit = Math.max(1, properties.getUploader().getPoll().getBatchSize());
            boolean ackMode = properties.getUploader().getAck().isEnabled();
            if (ackMode && mqttPublisher.getClientManager() != null) {
                mqttPublisher.getClientManager().ensureAckSubscription(ship.mmsi());
            }

            List<Map<String, Object>> rows = jdbcTemplate.query(
                    "SELECT * FROM " + stream.tableName() + " WHERE id > ? ORDER BY id ASC LIMIT ?",
                    (rs, rowNum) -> toMap(rs),
                    lastId, limit
            );

            if (rows.isEmpty()) {
                // 本轮无新行：期间到达的 ACK 仍可能推进 watermark。
                if (ackMode) {
                    advanceWatermark(jdbcTemplate, ship, stream, lastId);
                }
                return;
            }

            long maxSuccessId = lastId;
            int successCount = 0;
            boolean batchHasFailure = false;

            for (Map<String, Object> row : rows) {
                Object idObj = row.get("id");
                long rowId = (idObj instanceof Number num) ? num.longValue() : Long.parseLong(String.valueOf(idObj));
                prepareRow(ship, row);

                if (ackMode && ackTracker.inFlightCount(ship.mmsi(), stream.tableName())
                        >= Math.max(1, properties.getUploader().getAck().getMaxInFlight())) {
                    // 有限滑动窗口：等 ACK 回来再发新消息，禁止无界在途。
                    log.info("[Uploader] 在途窗口已满，等 ACK 后再发: table={}, mmsi={}",
                            stream.tableName(), ship.mmsi());
                    break;
                }
                if (ackMode && ackTracker.isTracked(ship.mmsi(), stream.tableName(), rowId)) {
                    // 已在途（或已 ACK 待 watermark）：主循环只发新行，超时未达由补发专路处理。
                    continue;
                }

                boolean ok = mqttPublisher.publish(ship.mmsi(), stream.type(), stream.topicSuffix(), row);
                if (ok) {
                    if (ackMode) {
                        // PUBACK 只记 IN_FLIGHT，游标不动，等 Application ACK。
                        ackTracker.track(ship.mmsi(), stream.tableName(), rowId,
                                MqttPublisher.stableMessageId(ship.mmsi(), stream.type(), row));
                    } else {
                        maxSuccessId = Math.max(maxSuccessId, rowId);
                    }
                    successCount++;
                    if (metrics != null) {
                        metrics.recordUploadRows(stream.streamKey(), true, 1);
                    }
                } else {
                    batchHasFailure = true;
                    if (metrics != null) {
                        metrics.recordUploadRows(stream.streamKey(), false, 1);
                    }
                    // 弱网断网保护：立即 break 跳出，绝不继续发送，完整保留断点！
                    log.warn("[Uploader] MQTT 发送失败，短路中断当前批次以冻结续传断点: table={}, failId={}",
                            stream.tableName(), rowId);
                    break;
                }
            }

            if (ackMode) {
                resendTimedOut(jdbcTemplate, ship, stream, limit);
                advanceWatermark(jdbcTemplate, ship, stream, lastId);
            } else if (maxSuccessId > lastId) {
                // 旧语义（ACK 关闭）：PUBACK 即推进游标。
                updateCursorId(jdbcTemplate, stream.tableName(), maxSuccessId);
                log.info("[Uploader] 上传成功: shipId={}, table={}, count={}, 游标推进: {} -> {}",
                        ship.shipId(), stream.tableName(), successCount, lastId, maxSuccessId);
            }

            if (metrics != null) {
                batchRecorded = true;
                metrics.recordUploadBatch(stream.streamKey(), !batchHasFailure);
            }
        } catch (Exception e) {
            if (!batchRecorded && metrics != null) {
                metrics.recordUploadBatch(stream.streamKey(), false);
            }
            log.warn("[Uploader] 增量表查询/上报异常: table={}, err={}", stream.tableName(), e.getMessage());
        }
    }

    /** 连续 ACK watermark 能推进才写游标；乱序缺口前停住等补发。 */
    private void advanceWatermark(JdbcTemplate jdbcTemplate, ShipDataSourceManager.ShipDatabase ship,
                                 IncrementalStream stream, long lastId) {
        long watermark = ackTracker.watermark(ship.mmsi(), stream.tableName(), lastId);
        if (watermark > lastId) {
            updateCursorId(jdbcTemplate, stream.tableName(), watermark);
            log.info("[Uploader] ACK watermark 推进: shipId={}, table={}, 游标: {} -> {}",
                    ship.shipId(), stream.tableName(), lastId, watermark);
        }
    }

    /**
     * 超时未 ACK 的补发（限本轮 batchSize 条）：按 id 回查原行重发，msg_id 由业务键
     * 确定故与之前完全相同，岸端去重吸收。行已不在（被清理）则放弃跟踪。
     */
    private void resendTimedOut(JdbcTemplate jdbcTemplate, ShipDataSourceManager.ShipDatabase ship,
                                IncrementalStream stream, int limit) {
        List<UploadAckTracker.Tracked> due = ackTracker.resendDue(
                ship.mmsi(), stream.tableName(),
                properties.getUploader().getAck().getAckTimeoutMs());
        int n = Math.min(due.size(), Math.max(1, limit));
        for (int i = 0; i < n; i++) {
            UploadAckTracker.Tracked t = due.get(i);
            Map<String, Object> row = queryRowById(jdbcTemplate, stream.tableName(), t.rowId());
            if (row == null) {
                log.warn("[Uploader] 补发行已不在，放弃跟踪: table={}, id={}",
                        stream.tableName(), t.rowId());
                ackTracker.forget(ship.mmsi(), stream.tableName(), t.rowId());
                continue;
            }
            prepareRow(ship, row);
            boolean ok = mqttPublisher.publish(ship.mmsi(), stream.type(), stream.topicSuffix(), row);
            if (ok) {
                ackTracker.track(ship.mmsi(), stream.tableName(), t.rowId(), t.msgId());
                if (metrics != null) {
                    metrics.recordUploadRows(stream.streamKey(), true, 1);
                }
            } else {
                if (metrics != null) {
                    metrics.recordUploadRows(stream.streamKey(), false, 1);
                }
                log.warn("[Uploader] 补发失败，短路等待下轮: table={}, failId={}",
                        stream.tableName(), t.rowId());
                break;
            }
        }
    }

    private Map<String, Object> queryRowById(JdbcTemplate jdbcTemplate, String table, long rowId) {
        List<Map<String, Object>> rows = jdbcTemplate.query(
                "SELECT * FROM " + table + " WHERE id = ?",
                (rs, rowNum) -> toMap(rs),
                rowId);
        return rows.isEmpty() ? null : rows.get(0);
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
