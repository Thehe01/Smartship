package com.smartship.edge.observability;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PoolSnapshot;
import com.smartship.edge.routing.ShipDataSourceManager;
import com.smartship.edge.routing.pool.MonitoredCallerRunsPolicy;
import com.smartship.edge.routing.pool.PersistenceAsyncConfig;
import com.smartship.edge.routing.pool.PersistencePoolMetrics;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import com.smartship.edge.uploader.DatabaseUploadPoller;
import com.smartship.edge.uploader.mqtt.MqttClientManager;
import com.smartship.edge.uploader.mqtt.MqttPublisher;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class ObservabilityMetricsTest {

    private SimpleMeterRegistry registry;
    private SmartShipMetrics smartShipMetrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        smartShipMetrics = new SmartShipMetrics(registry);
    }

    @Test
    @DisplayName("测试场景 1: Persistence Pool Gauge 与 PersistencePoolMetrics 实际值实时一致")
    void testPersistencePoolMetricsGaugesMatchRealValues() throws InterruptedException {
        EdgeProperties properties = new EdgeProperties();
        PersistenceAsyncConfig config = new PersistenceAsyncConfig(properties);
        MonitoredCallerRunsPolicy policy = config.persistenceRejectionPolicy();
        ThreadPoolTaskExecutor executor = config.persistenceExecutor(policy);

        try {
            PersistencePoolMetrics poolMetrics = new PersistencePoolMetrics(executor, policy);
            PersistenceMetricsBinder binder = new PersistenceMetricsBinder(poolMetrics);
            binder.bindTo(registry);

            assertEquals(0.0, registry.get("smartship_persistence_pool_active_threads").gauge().value());
            assertEquals(executor.getPoolSize(), registry.get("smartship_persistence_pool_size").gauge().value());
            assertEquals(0.0, registry.get("smartship_persistence_pool_queue_size").gauge().value());
            assertEquals(500.0, registry.get("smartship_persistence_pool_queue_remaining").gauge().value());
            assertEquals(0.0, registry.get("smartship_persistence_pool_completed_tasks").gauge().value());
            assertEquals(0.0, registry.get("smartship_persistence_pool_rejections_total").gauge().value());

            // 提交一个任务并等待其完成
            CountDownLatch latch = new CountDownLatch(1);
            executor.submit(latch::countDown);
            assertTrue(latch.await(2, TimeUnit.SECONDS));
            Thread.sleep(50); // 等待线程池内部 completedTaskCount 更新

            assertEquals(1.0, registry.get("smartship_persistence_pool_completed_tasks").gauge().value());
        } finally {
            executor.destroy();
        }
    }

    @Test
    @DisplayName("测试场景 2: CallerRuns 降级后 rejection 指标增加")
    void testCallerRunsRejectionMetricIncrements() {
        EdgeProperties properties = new EdgeProperties();
        PersistenceAsyncConfig config = new PersistenceAsyncConfig(properties);
        MonitoredCallerRunsPolicy policy = config.persistenceRejectionPolicy();
        ThreadPoolTaskExecutor executor = config.persistenceExecutor(policy);

        try {
            PersistencePoolMetrics poolMetrics = new PersistencePoolMetrics(executor, policy);
            PersistenceMetricsBinder binder = new PersistenceMetricsBinder(poolMetrics);
            binder.bindTo(registry);

            assertEquals(0.0, registry.get("smartship_persistence_pool_rejections_total").gauge().value());

            // 触发一次 CallerRuns 拒绝策略
            policy.rejectedExecution(() -> {}, executor.getThreadPoolExecutor());
            assertEquals(1.0, registry.get("smartship_persistence_pool_rejections_total").gauge().value());

            // 再次触发
            policy.rejectedExecution(() -> {}, executor.getThreadPoolExecutor());
            assertEquals(2.0, registry.get("smartship_persistence_pool_rejections_total").gauge().value());
        } finally {
            executor.destroy();
        }
    }

    @Test
    @DisplayName("测试场景 3: 持久化成功/失败分别累加对应 Counter 且 Timer 记录执行耗时")
    void testPersistenceSuccessFailureAndTimer() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:persist_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "");
        JdbcTemplate jt = new JdbcTemplate(ds);
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_gps_data (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    ship_id VARCHAR(64),
                    mmsi VARCHAR(32),
                    sentence_type VARCHAR(16),
                    source VARCHAR(32),
                    timestamp DATETIME,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    speed_knots DOUBLE,
                    course_over_ground DOUBLE,
                    heading_true DOUBLE,
                    heading_magnetic DOUBLE,
                    magnetic_variation DOUBLE,
                    altitude_m DOUBLE,
                    satellites INT,
                    hdop DOUBLE,
                    position_quality INT,
                    gps_status VARCHAR(16)
                )
                """);

        ShipDataSourceManager manager = mock(ShipDataSourceManager.class);
        when(manager.getJdbcTemplate(any(), eq("413999999"))).thenReturn(jt);

        EdgeProperties properties = new EdgeProperties();
        properties.setSchemaReady(true);
        properties.getCollect().getPersist().setEnabled(true);

        NmeaDataPersistenceService service = new NmeaDataPersistenceService(
                manager, properties, null, smartShipMetrics);

        // 1. 成功落库
        service.saveGps("RMC", "SERIAL", 31.2, 121.5, 12.0, 180.0,
                180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A", "413999999");

        Counter successCounter = registry.find("smartship_persistence_writes_total")
                .tag("type", "gps")
                .tag("result", "success")
                .counter();
        assertNotNull(successCounter);
        assertEquals(1.0, successCounter.count());

        Timer timer = registry.find("smartship_persistence_write_duration_seconds")
                .tag("type", "gps")
                .timer();
        assertNotNull(timer);
        assertEquals(1, timer.count());

        // 2. 失败落库（模拟执行 SQL 异常）
        JdbcTemplate errorJt = mock(JdbcTemplate.class);
        when(errorJt.update(anyString(), any(Object[].class))).thenThrow(new RuntimeException("DB down"));
        when(manager.getJdbcTemplate(any(), eq("413888888"))).thenReturn(errorJt);

        service.saveGps("RMC", "SERIAL", 31.2, 121.5, 12.0, 180.0,
                180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A", "413888888");

        Counter failureCounter = registry.find("smartship_persistence_writes_total")
                .tag("type", "gps")
                .tag("result", "failure")
                .counter();
        assertNotNull(failureCounter);
        assertEquals(1.0, failureCounter.count());
        assertEquals(2, timer.count(), "Timer 应统计两次尝试耗时");

        // 3. 校验跳过场景不计入 success / failure（persist disabled / mmsi null / schema not ready）
        properties.getCollect().getPersist().setEnabled(false);
        service.saveGps("RMC", "SERIAL", 31.2, 121.5, 12.0, 180.0,
                180.0, 180.0, 0.0, 10.0, 8, 1.0, 1, "A", "413999999");
        assertEquals(1.0, successCounter.count(), "禁用持久化跳过时不计入指标");
        assertEquals(1.0, failureCounter.count());
    }

    @Test
    @DisplayName("测试场景 4: 多船 HikariCP 全局聚合 Gauge 准确汇总多个连接池")
    void testDynamicHikariAggregateGaugesReflectMultiplePools() {
        ShipDataSourceManager manager = mock(ShipDataSourceManager.class);
        PoolSnapshot snap1 = new PoolSnapshot("ship-db-413000001", 5, 2, 3, 1, false, System.currentTimeMillis());
        PoolSnapshot snap2 = new PoolSnapshot("ship-db-413000002", 5, 1, 4, 0, false, System.currentTimeMillis());

        when(manager.listPoolSnapshots()).thenReturn(List.of(snap1, snap2));

        DataSourceMetricsBinder binder = new DataSourceMetricsBinder(manager);
        binder.bindTo(registry);

        assertEquals(2.0, registry.get("smartship_datasource_pool_count").gauge().value());
        assertEquals(10.0, registry.get("smartship_datasource_connections_total").gauge().value());
        assertEquals(3.0, registry.get("smartship_datasource_connections_active").gauge().value());
        assertEquals(7.0, registry.get("smartship_datasource_connections_idle").gauge().value());
        assertEquals(1.0, registry.get("smartship_datasource_threads_pending").gauge().value());
    }

    @Test
    @DisplayName("测试场景 5: MQTT 指标在连接与发布成功/失败时正确统计，且暴露 Gauge 与 Duration")
    void testMqttMetricsSuccessFailureAndDuration() {
        EdgeProperties properties = new EdgeProperties();
        properties.getUploader().getMqtt().setEnabled(true);
        properties.getUploader().getMqtt().setBrokerUrl("tcp://127.0.0.1:1883");

        MqttClientManager clientManager = new MqttClientManager(properties, smartShipMetrics);
        clientManager.bindTo(registry);

        // 未连接状态 Gauge
        Gauge connectedGauge = registry.find("smartship_mqtt_connected").gauge();
        assertNotNull(connectedGauge);
        assertEquals(0.0, connectedGauge.value());

        // 发布失败（因未连接且不可达）
        boolean published = clientManager.publish("test/topic", "hello".getBytes());
        assertFalse(published);

        Counter publishFailCounter = registry.find("smartship_mqtt_publish_total")
                .tag("result", "failure")
                .counter();
        assertNotNull(publishFailCounter);
        assertEquals(1.0, publishFailCounter.count());

        Timer publishTimer = registry.find("smartship_mqtt_publish_duration_seconds").timer();
        assertNotNull(publishTimer);
        assertTrue(publishTimer.count() >= 1);

        // connect 失败计数应已有 1 次（来自 publish 中尝试自动连接失败）
        Counter connectFailCounter = registry.find("smartship_mqtt_connect_total")
                .tag("result", "failure")
                .counter();
        assertNotNull(connectFailCounter);
        assertEquals(1.0, connectFailCounter.count());

        // 显式调用 recordMqttConnect(false) 后应变为 2.0
        smartShipMetrics.recordMqttConnect(false);
        assertEquals(2.0, connectFailCounter.count());

        // 模拟连接成功
        smartShipMetrics.recordMqttConnect(true);
        assertEquals(1.0, registry.find("smartship_mqtt_connect_total").tag("result", "success").counter().count());

        smartShipMetrics.recordMqttConnectionLost();
        assertEquals(1.0, registry.find("smartship_mqtt_connection_lost_total").counter().count());

        smartShipMetrics.recordMqttReconnect();
        assertEquals(1.0, registry.find("smartship_mqtt_reconnect_total").counter().count());
    }

    @Test
    @DisplayName("测试场景 6: Uploader 上报指标正常统计，且在失败时绝不破坏游标语义")
    void testUploaderSuccessFailureMetricsAndCursorSemantics() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:uploader_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "");
        JdbcTemplate jt = new JdbcTemplate(ds);
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_gps_data (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    ship_id VARCHAR(64),
                    mmsi VARCHAR(32),
                    sentence_type VARCHAR(16)
                )
                """);
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_upload_cursor (
                    stream_name VARCHAR(64) NOT NULL,
                    partition_key VARCHAR(64) NOT NULL DEFAULT '',
                    last_uploaded_id BIGINT NOT NULL DEFAULT 0,
                    last_uploaded_time DATETIME NULL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stream_name, partition_key)
                )
                """);

        // 插入 3 条数据 (id 1, 2, 3)
        jt.execute("INSERT INTO zncb_gps_data (id, ship_id, mmsi, sentence_type) VALUES (1, 's1', '413999999', 'RMC')");
        jt.execute("INSERT INTO zncb_gps_data (id, ship_id, mmsi, sentence_type) VALUES (2, 's1', '413999999', 'GGA')");
        jt.execute("INSERT INTO zncb_gps_data (id, ship_id, mmsi, sentence_type) VALUES (3, 's1', '413999999', 'VTG')");

        EdgeProperties properties = new EdgeProperties();
        properties.getUploader().setEnabled(true);
        properties.getUploader().getPoll().setBatchSize(10);

        MqttPublisher mqttPublisher = mock(MqttPublisher.class);
        when(mqttPublisher.publish(anyString(), anyString(), anyString(), anyMap())).thenReturn(true);

        ShipDataSourceManager shipDataSourceManager = mock(ShipDataSourceManager.class);
        ShipDataSourceManager.ShipDatabase ship = new ShipDataSourceManager.ShipDatabase(
                "s1", "413999999", "zncb_ship", "localhost", 3306, "root", "123", true
        );

        DatabaseUploadPoller poller = new DatabaseUploadPoller(
                properties, shipDataSourceManager, mqttPublisher, smartShipMetrics, null
        );

        DatabaseUploadPoller.IncrementalStream gpsStream = new DatabaseUploadPoller.IncrementalStream(
                "gps", "zncb_gps_data", "nmea_gps", "nmea_gps"
        );

        // 第一批上传全部成功
        poller.uploadIncrementalStream(jt, ship, gpsStream);

        Long cursorAfterFirst = jt.queryForObject(
                "SELECT last_uploaded_id FROM zncb_upload_cursor WHERE stream_name = 'zncb_gps_data'", Long.class);
        assertEquals(3L, cursorAfterFirst, "游标应推进到 3");

        Counter rowsSuccess = registry.find("smartship_uploader_rows_total")
                .tag("stream", "gps")
                .tag("result", "success")
                .counter();
        assertNotNull(rowsSuccess);
        assertEquals(3.0, rowsSuccess.count());

        Counter batchSuccess = registry.find("smartship_uploader_batches_total")
                .tag("stream", "gps")
                .tag("result", "success")
                .counter();
        assertNotNull(batchSuccess);
        assertEquals(1.0, batchSuccess.count());

        // 第二批：追加 id 4, 5，其中 id 4 成功，id 5 发送失败
        jt.execute("INSERT INTO zncb_gps_data (id, ship_id, mmsi, sentence_type) VALUES (4, 's1', '413999999', 'RMC')");
        jt.execute("INSERT INTO zncb_gps_data (id, ship_id, mmsi, sentence_type) VALUES (5, 's1', '413999999', 'GGA')");

        when(mqttPublisher.publish(eq("413999999"), eq("nmea_gps"), eq("nmea_gps"), argThat(map -> Long.valueOf(4).equals(map.get("id")))))
                .thenReturn(true);
        when(mqttPublisher.publish(eq("413999999"), eq("nmea_gps"), eq("nmea_gps"), argThat(map -> Long.valueOf(5).equals(map.get("id")))))
                .thenReturn(false);

        poller.uploadIncrementalStream(jt, ship, gpsStream);

        Long cursorAfterSecond = jt.queryForObject(
                "SELECT last_uploaded_id FROM zncb_upload_cursor WHERE stream_name = 'zncb_gps_data'", Long.class);
        assertEquals(4L, cursorAfterSecond, "游标应精确冻结在成功断点 4，绝不推进到 5");

        assertEquals(4.0, rowsSuccess.count());

        Counter rowsFailure = registry.find("smartship_uploader_rows_total")
                .tag("stream", "gps")
                .tag("result", "failure")
                .counter();
        assertNotNull(rowsFailure);
        assertEquals(1.0, rowsFailure.count());

        Counter batchFailure = registry.find("smartship_uploader_batches_total")
                .tag("stream", "gps")
                .tag("result", "failure")
                .counter();
        assertNotNull(batchFailure);
        assertEquals(1.0, batchFailure.count());
    }

    @Test
    @DisplayName("测试场景 7: Upload Backlog 计算、异常容错与历史值保留")
    void testUploadBacklogCalculationAndFaultTolerance() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:backlog_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "");
        JdbcTemplate jt = new JdbcTemplate(ds);
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_gps_data (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    sentence_type VARCHAR(16)
                )
                """);
        jt.execute("""
                CREATE TABLE IF NOT EXISTS zncb_upload_cursor (
                    stream_name VARCHAR(64) NOT NULL,
                    partition_key VARCHAR(64) NOT NULL DEFAULT '',
                    last_uploaded_id BIGINT NOT NULL DEFAULT 0,
                    PRIMARY KEY (stream_name, partition_key)
                )
                """);

        // maxId = 10, cursor = 4 -> backlog = 6
        for (int i = 1; i <= 10; i++) {
            jt.execute("INSERT INTO zncb_gps_data (id, sentence_type) VALUES (" + i + ", 'RMC')");
        }
        jt.execute("INSERT INTO zncb_upload_cursor (stream_name, partition_key, last_uploaded_id) VALUES ('zncb_gps_data', '', 4)");

        ShipDataSourceManager manager = mock(ShipDataSourceManager.class);
        ShipDataSourceManager.ShipDatabase ship = new ShipDataSourceManager.ShipDatabase(
                "s1", "413999999", "zncb_ship", "localhost", 3306, "root", "123", true
        );
        when(manager.listEnabledRegistries()).thenReturn(List.of(ship));
        when(manager.getJdbcTemplate("s1", "413999999")).thenReturn(jt);

        UploadBacklogMetrics backlogMetrics = new UploadBacklogMetrics(manager);
        backlogMetrics.bindTo(registry);

        Gauge backlogGauge = registry.find("smartship_uploader_backlog_rows").gauge();
        assertNotNull(backlogGauge);
        assertEquals(6.0, backlogGauge.value(), "初始 backlog 应为 10 - 4 = 6");

        // 推进 cursor 至 10 -> backlog 应为 0
        jt.execute("UPDATE zncb_upload_cursor SET last_uploaded_id = 10 WHERE stream_name = 'zncb_gps_data'");
        assertEquals(0.0, backlogGauge.value());

        // 模拟异常时保留历史值
        when(manager.getJdbcTemplate("s1", "413999999")).thenThrow(new RuntimeException("Connection lost"));
        // 再次获取应保留上一次的 0.0，绝不崩溃
        assertEquals(0.0, backlogGauge.value());
    }

    @Test
    @DisplayName("测试场景 8: 严格验证所有指标杜绝 MMSI / shipId 等高基数 Label")
    void testNoHighCardinalityTags() {
        // 记录各种指标
        smartShipMetrics.recordPersistenceSuccess("gps", 1000);
        smartShipMetrics.recordPersistenceFailure("wind", 1000);
        smartShipMetrics.recordMqttPublish(true, 500);
        smartShipMetrics.recordUploadRows("depth", true, 2);
        smartShipMetrics.recordUploadBatch("engine", true);

        Set<String> forbiddenTagKeys = Set.of(
                "mmsi", "ship_id", "shipid", "msg_id", "msgid", "topic", "table", "tablename"
        );

        Set<String> allowedStreamsOrTypes = Set.of(
                "gps", "wind", "depth", "rudder", "engine", "other", "unknown"
        );

        Set<String> allowedResults = Set.of(
                "success", "failure"
        );

        for (Meter meter : registry.getMeters()) {
            String meterName = meter.getId().getName();
            assertTrue(meterName.startsWith("smartship_"), "指标必须以 smartship_ 开头: " + meterName);

            for (Tag tag : meter.getId().getTags()) {
                String key = tag.getKey().toLowerCase();
                assertFalse(forbiddenTagKeys.contains(key),
                        "禁止在 Prometheus 指标中暴露高基数 Tag: " + key + " on " + meterName);

                if ("type".equals(key) || "stream".equals(key)) {
                    assertTrue(allowedStreamsOrTypes.contains(tag.getValue()),
                            "Tag " + key + " 的值必须为受控有限枚举: " + tag.getValue());
                }

                if ("result".equals(key)) {
                    assertTrue(allowedResults.contains(tag.getValue()),
                            "Tag result 的值必须为 success|failure: " + tag.getValue());
                }
            }
        }
    }
}
