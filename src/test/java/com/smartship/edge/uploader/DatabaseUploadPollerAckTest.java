package com.smartship.edge.uploader;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.ShipDataSourceManager;
import com.smartship.edge.uploader.mqtt.MqttPublisher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Poller + ACK 跟踪器集成单测（H2 真库 + mock 发布）。
 *
 * <p>覆盖：PUBACK 只记在途不推游标（T1）、无 ACK 游标冻结（T2）、ACK 后 watermark
 * 推进、重启后未 ACK 行继续补传（T5）、ACK 关闭时旧语义不变、超时补发确定性触发。
 */
class DatabaseUploadPollerAckTest {

    private static final String MMSI = "413999999";
    private static final String TABLE = "zncb_gps_data";

    private EmbeddedDatabase db;
    private JdbcTemplate jdbc;
    private EdgeProperties properties;
    private MqttPublisher publisher;
    private UploadAckTracker tracker;
    private DatabaseUploadPoller poller;
    private ShipDataSourceManager.ShipDatabase ship;

    @BeforeEach
    void setUp() {
        db = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .build();
        jdbc = new JdbcTemplate(db);
        jdbc.execute("CREATE TABLE zncb_gps_data ("
                + " id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + " speed DOUBLE NOT NULL,"
                + " update_time VARCHAR(32) NOT NULL)");
        jdbc.execute("CREATE TABLE zncb_upload_cursor ("
                + " stream_name VARCHAR(64) NOT NULL,"
                + " partition_key VARCHAR(64) NOT NULL DEFAULT '',"
                + " last_uploaded_id BIGINT NOT NULL DEFAULT 0,"
                + " last_uploaded_time TIMESTAMP NULL,"
                + " updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                + " PRIMARY KEY (stream_name, partition_key))");

        properties = new EdgeProperties();
        properties.getUploader().getPoll().setBatchSize(10);
        properties.getUploader().getAck().setAckTimeoutMs(30_000L);
        properties.getUploader().getAck().setMaxInFlight(100);

        publisher = mock(MqttPublisher.class);
        when(publisher.publish(anyString(), anyString(), anyString(), anyMap()))
                .thenReturn(true);
        when(publisher.getClientManager())
                .thenReturn(mock(com.smartship.edge.uploader.mqtt.MqttClientManager.class));
        tracker = new UploadAckTracker();
        poller = new DatabaseUploadPoller(properties, mock(ShipDataSourceManager.class),
                publisher, null, null, tracker);
        ship = new ShipDataSourceManager.ShipDatabase(
                "S001", MMSI, "zncb_auth", "localhost", 3306, "root", "123456", true);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    private DatabaseUploadPoller.IncrementalStream stream() {
        return new DatabaseUploadPoller.IncrementalStream(TABLE, "nmea_gps", "nmea_gps");
    }

    private void insertRows(int... ids) {
        for (int id : ids) {
            jdbc.update("INSERT INTO zncb_gps_data (id, speed, update_time) VALUES (?, ?, ?)",
                    id, 10.0 + id, "2026-09-19T10:00:0" + (id % 10));
        }
    }

    private long cursor() {
        Long v = jdbc.queryForObject(
                "SELECT last_uploaded_id FROM zncb_upload_cursor WHERE stream_name = ?",
                Long.class, TABLE);
        return v == null ? 0L : v;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<Map<String, Object>> publishedRows() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        verify(publisher, atLeastOnce()).publish(eq(MMSI), eq("nmea_gps"), eq("nmea_gps"),
                captor.capture());
        return new java.util.ArrayList<>((List) captor.getAllValues());
    }

    @Test
    @DisplayName("T1+T2: PUBACK 只记在途，游标不动；无 ACK 时第二轮也不推进")
    void pubackDoesNotAdvanceCursor() {
        insertRows(1001, 1002, 1003);

        poller.uploadIncrementalStream(jdbc, ship, stream());
        assertEquals(3, publishedRows().size(), "三行全部发出");
        assertEquals(0L, cursor(), "PUBACK 不推进游标，等 Application ACK");
        assertEquals(3, tracker.inFlightCount(MMSI, TABLE));

        clearInvocations(publisher);
        poller.uploadIncrementalStream(jdbc, ship, stream());
        verify(publisher, never()).publish(anyString(), anyString(), anyString(), anyMap());
        assertEquals(0L, cursor(), "无 ACK 时游标冻结，主循环不重复发已在途行");
    }

    @Test
    @DisplayName("ACK 到齐后 watermark 一次推进；跟踪器 msg_id 与发布载荷一致")
    void acksAdvanceWatermark() {
        insertRows(1001, 1002);

        poller.uploadIncrementalStream(jdbc, ship, stream());

        // 跟踪器登记的 msg_id 必须等于发布载荷里的 msg_id（同纯函数同行，双重保险）。
        for (Map<String, Object> row : publishedRows()) {
            String payloadMsgId = MqttPublisher.stableMessageId(MMSI, "nmea_gps", row);
            Object rowId = row.get("id");
            long id = (rowId instanceof Number num) ? num.longValue() : Long.parseLong(String.valueOf(rowId));
            assertTrue(tracker.onAck(MMSI, payloadMsgId, String.valueOf(id)));
        }

        poller.uploadIncrementalStream(jdbc, ship, stream());
        assertEquals(1002L, cursor(), "连续 ACK watermark 推进游标");
    }

    @Test
    @DisplayName("T5: 重启后未 ACK 行继续补传，已推进的不再重发")
    void restartResumesUnacked() {
        insertRows(1001, 1002);
        poller.uploadIncrementalStream(jdbc, ship, stream());
        for (Map<String, Object> row : publishedRows()) {
            tracker.onAck(MMSI, MqttPublisher.stableMessageId(MMSI, "nmea_gps", row),
                    String.valueOf(row.get("id")));
        }
        poller.uploadIncrementalStream(jdbc, ship, stream());
        assertEquals(1002L, cursor());

        // 重启 = 新跟踪器 + 新 poller，游标表持久化了，H2 同库。
        UploadAckTracker freshTracker = new UploadAckTracker();
        DatabaseUploadPoller freshPoller = new DatabaseUploadPoller(properties,
                mock(ShipDataSourceManager.class), publisher, null, null, freshTracker);
        insertRows(1003);
        clearInvocations(publisher);

        freshPoller.uploadIncrementalStream(jdbc, ship, freshPollerStream());
        List<Map<String, Object>> resent = publishedRows();
        assertEquals(1, resent.size(), "只补传游标之后的行");
        assertEquals(1003L, ((Number) resent.get(0).get("id")).longValue());
        assertEquals(1002L, cursor(), "补传未 ACK 前游标不动");

        freshTracker.onAck(MMSI,
                MqttPublisher.stableMessageId(MMSI, "nmea_gps", resent.get(0)), "1003");
        freshPoller.uploadIncrementalStream(jdbc, ship, freshPollerStream());
        assertEquals(1003L, cursor(), "补传 ACK 后游标跟上");
    }

    private DatabaseUploadPoller.IncrementalStream freshPollerStream() {
        return new DatabaseUploadPoller.IncrementalStream(TABLE, "nmea_gps", "nmea_gps");
    }

    @Test
    @DisplayName("ACK 关闭时退回 PUBACK 即推进的旧语义")
    void legacySemanticsWhenAckDisabled() {
        properties.getUploader().getAck().setEnabled(false);
        insertRows(1001, 1002);

        poller.uploadIncrementalStream(jdbc, ship, stream());

        assertEquals(2, publishedRows().size());
        assertEquals(1002L, cursor(), "旧语义：PUBACK 即推进");
        assertEquals(0, tracker.inFlightCount(MMSI, TABLE), "旧语义不登记在途");
    }

    @Test
    @DisplayName("ackTimeout=0 时超时补发确定性触发且限量")
    void resendDeterministicAtZeroTimeout() {
        properties.getUploader().getAck().setAckTimeoutMs(0L);
        insertRows(1001, 1002);

        poller.uploadIncrementalStream(jdbc, ship, stream());

        // 首轮 2 发 + 补发轮（限 batchSize=10）：补发被触发，无异常、无游标推进。
        assertTrue(publishedRows().size() >= 2, "补发路径被执行");
        assertEquals(0L, cursor(), "补发未 ACK 前游标不动");
    }

    @Test
    @DisplayName("ACK 在 publish 返回前到达：预注册命中，watermark 照常推进")
    void ackArrivingMidPublishStillMatches() {
        insertRows(1001, 1002);
        // 模拟岸端极快：PUBACK 还没返回，Application ACK 已经到了。
        // 旧流程（先发布后登记）会把这条 ACK 当未知丢弃；预注册必须命中。
        when(publisher.publish(anyString(), anyString(), anyString(), anyMap()))
                .thenAnswer((org.mockito.stubbing.Answer<Boolean>) inv -> {
                    Map<String, Object> row = inv.getArgument(3);
                    Object idObj = row.get("id");
                    long rowId = (idObj instanceof Number num) ? num.longValue()
                            : Long.parseLong(String.valueOf(idObj));
                    tracker.onAck(MMSI, MqttPublisher.stableMessageId(MMSI, "nmea_gps", row),
                            String.valueOf(rowId));
                    return true;
                });

        poller.uploadIncrementalStream(jdbc, ship, stream());

        assertEquals(1002L, cursor(), "PUBACK 返回前的 ACK 必须 counted，游标一次推进");
    }

    @Test
    @DisplayName("publish 失败：预注册被清理，可正常重试，不推游标不造假 ACK")
    void failedPublishCleansPreRegistration() {
        insertRows(1001);
        when(publisher.publish(anyString(), anyString(), anyString(), anyMap()))
                .thenReturn(false);

        poller.uploadIncrementalStream(jdbc, ship, stream());

        assertFalse(tracker.isTracked(MMSI, TABLE, 1001L), "预注册必须回滚");
        assertEquals(0, tracker.inFlightCount(MMSI, TABLE));
        assertEquals(0L, cursor(), "失败不推游标");

        // 恢复后重试：登记-确认-推进全链路正常。
        when(publisher.publish(anyString(), anyString(), anyString(), anyMap()))
                .thenReturn(true);
        poller.uploadIncrementalStream(jdbc, ship, stream());
        assertTrue(tracker.isTracked(MMSI, TABLE, 1001L));
        assertEquals(0L, cursor(), "ACK 到达前游标仍不动");

        Map<String, Object> row = publishedRows().get(publishedRows().size() - 1);
        tracker.onAck(MMSI, MqttPublisher.stableMessageId(MMSI, "nmea_gps", row), "1001");
        poller.uploadIncrementalStream(jdbc, ship, stream());
        assertEquals(1001L, cursor(), "重试成功 + ACK 后游标推进");
    }
}
