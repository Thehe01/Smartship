package com.smartship.edge.observability;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureObservability
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:actuator_test;DB_CLOSE_DELAY=-1;MODE=MySQL",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.sql.init.schema-locations=classpath:schema/auth-schema.sql",
        "spring.sql.init.mode=always",
        "management.prometheus.metrics.export.enabled=true",
        "management.endpoints.web.exposure.include=health,info,prometheus,metrics",
        "smartship.edge.collect.nmea.serial.enabled=false",
        "smartship.edge.collect.nmea.tcp.enabled=false",
        "smartship.edge.collect.nmea.udp.enabled=false",
        "smartship.edge.uploader.enabled=false",
        "smartship.edge.uploader.mqtt.enabled=false"
})
public class ActuatorPrometheusEndpointTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private SmartShipMetrics smartShipMetrics;

    @Test
    @DisplayName("测试 /actuator/health 端点正常暴露并返回 UP")
    void testActuatorHealthEndpoint() {
        ResponseEntity<String> response = restTemplate.getForEntity("/actuator/health", String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("UP"));
    }

    @Test
    @DisplayName("测试 /actuator/prometheus 端点成功输出 SmartShip 各子系统指标")
    void testActuatorPrometheusEndpoint() {
        // 触发业务指标埋点
        smartShipMetrics.recordPersistenceSuccess("gps", 1_500_000);
        smartShipMetrics.recordPersistenceFailure("wind", 2_000_000);
        smartShipMetrics.recordMqttPublish(true, 500_000);
        smartShipMetrics.recordMqttConnect(true);
        smartShipMetrics.recordUploadRows("gps", true, 5);
        smartShipMetrics.recordUploadBatch("gps", true);

        ResponseEntity<String> response = restTemplate.getForEntity("/actuator/prometheus", String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        String body = response.getBody();
        assertNotNull(body);

        // 验证持久化线程池 Gauge
        assertTrue(body.contains("smartship_persistence_pool_active_threads"),
                "Prometheus 必须暴露 smartship_persistence_pool_active_threads");
        assertTrue(body.contains("smartship_persistence_pool_size"),
                "Prometheus 必须暴露 smartship_persistence_pool_size");
        assertTrue(body.contains("smartship_persistence_pool_queue_size"),
                "Prometheus 必须暴露 smartship_persistence_pool_queue_size");
        assertTrue(body.contains("smartship_persistence_pool_queue_remaining"),
                "Prometheus 必须暴露 smartship_persistence_pool_queue_remaining");
        assertTrue(body.contains("smartship_persistence_pool_completed_tasks"),
                "Prometheus 必须暴露 smartship_persistence_pool_completed_tasks");
        assertTrue(body.contains("smartship_persistence_pool_rejections"),
                "Prometheus 必须暴露 smartship_persistence_pool_rejections");

        // 验证动态 HikariCP 聚合 Gauge
        assertTrue(body.contains("smartship_datasource_pool_count"),
                "Prometheus 必须暴露 smartship_datasource_pool_count");
        assertTrue(body.contains("smartship_datasource_connections"),
                "Prometheus 必须暴露 smartship_datasource_connections");
        assertTrue(body.contains("smartship_datasource_connections_active"),
                "Prometheus 必须暴露 smartship_datasource_connections_active");
        assertTrue(body.contains("smartship_datasource_connections_idle"),
                "Prometheus 必须暴露 smartship_datasource_connections_idle");
        assertTrue(body.contains("smartship_datasource_threads_pending"),
                "Prometheus 必须暴露 smartship_datasource_threads_pending");

        // 验证持久化业务指标
        assertTrue(body.contains("smartship_persistence_writes_total"),
                "Prometheus 必须暴露 smartship_persistence_writes_total");
        assertTrue(body.contains("type=\"gps\""), "标签 type 必须包含 gps");
        assertTrue(body.contains("type=\"wind\""), "标签 type 必须包含 wind");
        assertTrue(body.contains("result=\"success\""), "标签 result 必须包含 success");
        assertTrue(body.contains("result=\"failure\""), "标签 result 必须包含 failure");
        assertTrue(body.contains("smartship_persistence_write_duration_seconds"),
                "Prometheus 必须暴露 smartship_persistence_write_duration_seconds");

        // 验证 MQTT 网络指标
        assertTrue(body.contains("smartship_mqtt_connected"),
                "Prometheus 必须暴露 smartship_mqtt_connected");
        assertTrue(body.contains("smartship_mqtt_connect_total"),
                "Prometheus 必须暴露 smartship_mqtt_connect_total");
        assertTrue(body.contains("smartship_mqtt_publish_total"),
                "Prometheus 必须暴露 smartship_mqtt_publish_total");
        assertTrue(body.contains("smartship_mqtt_publish_duration_seconds"),
                "Prometheus 必须暴露 smartship_mqtt_publish_duration_seconds");

        // 验证 Uploader 指标
        assertTrue(body.contains("smartship_uploader_rows_total"),
                "Prometheus 必须暴露 smartship_uploader_rows_total");
        assertTrue(body.contains("stream=\"gps\""), "标签 stream 必须包含 gps");
        assertTrue(body.contains("smartship_uploader_batches_total"),
                "Prometheus 必须暴露 smartship_uploader_batches_total");
        assertTrue(body.contains("smartship_uploader_backlog_rows"),
                "Prometheus 必须暴露 smartship_uploader_backlog_rows");

        // 验证禁止出现高基数标签
        assertFalse(body.contains("mmsi="), "Prometheus 输出禁止暴露 mmsi 动态标签");
        assertFalse(body.contains("ship_id="), "Prometheus 输出禁止暴露 ship_id 动态标签");
    }
}
