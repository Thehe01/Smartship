package com.smartship.edge.benchmark;

import com.smartship.edge.benchmark.simulator.FaultInjectingMqttGateway;
import com.smartship.edge.benchmark.support.BenchmarkFixtures;
import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.ShipDataSourceManager;
import com.smartship.edge.uploader.DatabaseUploadPoller;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * P1-4 MQTT 弱网恢复 Benchmark（integration benchmark，非 JMH）。
 * <p>
 * 全程 offline-capable：使用 {@link FaultInjectingMqttGateway} 脚本化成功/失败，
 * 真实驱动 {@code DatabaseUploadPoller} 游标推进、break-on-failure 与断点续传。
 * 系统语义为 at-least-once，本测试只验证重传与 msg_id 确定性，不宣称 exactly-once。
 */
@Tag("benchmark")
@DisplayName("P1-4 Benchmark: MQTT recovery")
class MqttRecoveryBenchmarkTest {

    private static final String SHIP_ID = "ship-bench";
    private static final String MMSI = "413999999";
    private static final String TABLE = "zncb_gps_data";
    private static final BenchmarkRecorder RECORDER = new BenchmarkRecorder(params());

    private static Map<String, String> params() {
        return Map.of("benchmark.mqtt.rows", prop("benchmark.mqtt.rows", "10000"));
    }

    private static String prop(String key, String def) {
        return System.getProperty(key, def);
    }

    private static int mqttRows() {
        return Integer.parseInt(prop("benchmark.mqtt.rows", "10000"));
    }

    @AfterAll
    static void flushReport() {
        RECORDER.flush();
    }

    private static final class Fixture {
        final JdbcTemplate jt;
        final DatabaseUploadPoller poller;
        final DatabaseUploadPoller.IncrementalStream stream;
        final ShipDataSourceManager.ShipDatabase ship;
        final FaultInjectingMqttGateway gateway;

        Fixture(int rows, int batchSize) {
            jt = BenchmarkFixtures.newH2("mqtt_bench_" + System.nanoTime());
            BenchmarkFixtures.createShipTables(jt);
            for (int i = 0; i < rows; i += 1000) {
                int end = Math.min(i + 1000, rows);
                StringBuilder sb = new StringBuilder(
                        "INSERT INTO zncb_gps_data (ship_id, mmsi, sentence_type) VALUES ");
                for (int id = i + 1; id <= end; id++) {
                    if (id > i + 1) {
                        sb.append(',');
                    }
                    sb.append("('").append(SHIP_ID).append("','").append(MMSI).append("','RMC')");
                }
                jt.execute(sb.toString());
            }
            jt.update("INSERT INTO zncb_upload_cursor (stream_name, partition_key, last_uploaded_id)"
                    + " VALUES (?, '', 0)", TABLE);

            EdgeProperties properties = new EdgeProperties();
            properties.getUploader().setEnabled(true);
            properties.getUploader().getPoll().setBatchSize(batchSize);
            gateway = new FaultInjectingMqttGateway();
            ShipDataSourceManager manager = mock(ShipDataSourceManager.class);
            ship = new ShipDataSourceManager.ShipDatabase(
                    SHIP_ID, MMSI, "mqtt_bench_db", "localhost", 3306, "sa", "", true);
            when(manager.listEnabledRegistries()).thenReturn(List.of(ship));
            when(manager.getJdbcTemplate(any(), any())).thenReturn(jt);
            poller = new DatabaseUploadPoller(properties, manager, gateway.publisher());
            stream = new DatabaseUploadPoller.IncrementalStream("gps", TABLE, "nmea_gps", "nmea_gps");
        }

        long cursor() {
            Long v = jt.queryForObject(
                    "SELECT last_uploaded_id FROM zncb_upload_cursor WHERE stream_name = ?", Long.class, TABLE);
            return v == null ? 0L : v;
        }

        long backlog() {
            return BenchmarkFixtures.maxId(jt, TABLE) - cursor();
        }

        /** 反复驱动增量上传直到游标不再推进，返回驱动轮数。 */
        int driveToQuiescence() {
            int rounds = 0;
            while (rounds < 500) {
                long before = cursor();
                poller.uploadIncrementalStream(jt, ship, stream);
                rounds++;
                if (cursor() == before) {
                    break;
                }
            }
            return rounds;
        }
    }

    @Test
    @DisplayName("mqtt-outage-recovery: 断网冻结游标，恢复后从断点续传至 zero backlog")
    void mqttOutageRecovery() {
        int rows = mqttRows();
        int failAfter = Math.min(3000, rows - 1);
        BenchmarkRecorder.Scenario s = RECORDER.scenario("mqtt-outage-recovery")
                .param("pending_rows", rows)
                .param("batch_size", 1000)
                .param("fail_after_rows", failAfter)
                .note("at-least-once：失败即 break 冻结游标；恢复后从 last_uploaded_id 续查，不重扫已确认区间");
        Fixture f = new Fixture(rows, 1000);
        s.count("initial_backlog", f.backlog());
        assertEquals(rows, f.backlog());

        // 阶段一：断网
        f.gateway.succeedFirstNThenFail(failAfter);
        f.driveToQuiescence();
        assertEquals(failAfter, f.cursor(), "断网后游标必须冻结在最后一个成功断点");
        assertEquals(rows - failAfter, f.backlog(), "剩余积压必须完整保留");
        s.count("outage_cursor", f.cursor())
                .count("outage_backlog_peak", f.backlog());

        // 阶段二：恢复
        f.gateway.alwaysSucceed();
        int attemptsBefore = f.gateway.attempts();
        long t0 = System.nanoTime();
        f.driveToQuiescence();
        long recoveryMs = (System.nanoTime() - t0) / 1_000_000L;
        assertEquals(rows, f.cursor(), "恢复后游标必须推进到末尾");
        assertEquals(0, f.backlog(), "恢复后积压必须归零");
        s.count("rows_replayed", f.gateway.attempts() - attemptsBefore)
                .count("final_cursor", f.cursor())
                .count("final_backlog", f.backlog())
                .observePeak("recovery_ms", recoveryMs);
        RECORDER.complete(s);
    }

    @Test
    @DisplayName("mqtt-crash-window: publish 成功但 cursor 未提交时重传且 msg_id 稳定")
    void mqttCrashWindow() throws Exception {
        int rows = 2000;
        BenchmarkRecorder.Scenario s = RECORDER.scenario("mqtt-crash-window")
                .param("pending_rows", rows)
                .param("batch_size", 500)
                .note("模拟 publish 成功后 cursor 更新前 crash：消息重传，msg_id 恒定，岸端可幂等");
        Fixture f = new Fixture(rows, 500);
        f.gateway.alwaysSucceed();
        // 用确定性子类模拟 crash 窗口：publish 成功后、cursor 提交前抛异常。
        // 注意不用 Mockito 桩 update(String, Object...) varargs 重载（重载匹配易 miss），
        // 子类覆写无歧义且只拦截 cursor 提交语句。
        javax.sql.DataSource ds = f.jt.getDataSource();
        assertNotNull(ds);
        java.util.concurrent.atomic.AtomicBoolean crashed = new java.util.concurrent.atomic.AtomicBoolean(false);
        JdbcTemplate crashOnceJt = new JdbcTemplate(ds) {
            @Override
            public int update(String sql, Object... args) throws DataAccessException {
                if (!crashed.getAndSet(true) && sql.startsWith("UPDATE zncb_upload_cursor")) {
                    throw new UncategorizedSQLException("crash-window", sql,
                            new java.sql.SQLException("simulated crash before cursor commit"));
                }
                return super.update(sql, args);
            }
        };

        ShipDataSourceManager manager = mock(ShipDataSourceManager.class);
        when(manager.getJdbcTemplate(any(), any())).thenReturn(crashOnceJt);
        EdgeProperties properties = new EdgeProperties();
        properties.getUploader().setEnabled(true);
        properties.getUploader().getPoll().setBatchSize(500);
        DatabaseUploadPoller crashingPoller =
                new DatabaseUploadPoller(properties, manager, f.gateway.publisher());

        // 第一次：500 条 publish 成功，cursor 提交失败
        crashingPoller.uploadIncrementalStream(crashOnceJt, f.ship, f.stream);
        assertEquals(500, f.gateway.attempts(), "首批 500 条必须已 publish");
        assertEquals(0, f.cursor(), "crash 后游标必须保持 0");
        List<String> firstIds = f.gateway.publishedForId(1L).stream()
                .map(FaultInjectingMqttGateway.Published::msgId).toList();
        assertEquals(1, firstIds.size());

        // 第二次：同样 poller、全量重跑，id=1 的 msg_id 必须与第一次完全相同
        DatabaseUploadPoller recoveredPoller =
                new DatabaseUploadPoller(properties, manager, f.gateway.publisher());
        int rounds = 0;
        while (f.cursor() < rows && rounds < 50) {
            recoveredPoller.uploadIncrementalStream(crashOnceJt, f.ship, f.stream);
            rounds++;
        }
        assertEquals(rows, f.cursor());
        List<String> allIds = f.gateway.publishedForId(1L).stream()
                .map(FaultInjectingMqttGateway.Published::msgId).toList();
        assertTrue(allIds.size() >= 2, "id=1 必须被重传");
        assertTrue(allIds.stream().allMatch(id -> id.equals(firstIds.get(0))),
                "重传 msg_id 必须恒定，证明 at-least-once + 确定性 ID");
        s.count("first_attempt_publishes", 500)
                .count("redelivered_id1_times", allIds.size())
                .count("final_cursor", f.cursor())
                .count("msg_id_stable", 1);
        RECORDER.complete(s);
    }
}
