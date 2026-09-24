package com.smartship.edge.uploader.mqtt;

import com.smartship.edge.config.EdgeProperties;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ACK 分发门：只认 {@code status == KAFKA_COMMITTED}，其余一律忽略；
 * msg_id 必填、seq 可选保持不变。
 */
class MqttAckDispatchTest {

    record Received(String topic, String msgId, String seq) {
    }

    private MqttClientManager managerWith(List<Received> out) {
        MqttClientManager manager = new MqttClientManager(new EdgeProperties());
        manager.setAckListener((topic, msgId, seq) -> out.add(new Received(topic, msgId, seq)));
        return manager;
    }

    private static MqttMessage message(String json) {
        return new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("KAFKA_COMMITTED 分发，msg_id/seq 原样透传")
    void committedDispatched() {
        List<Received> out = new CopyOnWriteArrayList<>();
        MqttClientManager manager = managerWith(out);

        manager.dispatchAck("ship/413999999/ack", message(
                "{\"msg_id\":\"abc\",\"seq\":\"1001\",\"status\":\"KAFKA_COMMITTED\"}"));

        assertEquals(1, out.size());
        assertEquals("ship/413999999/ack", out.get(0).topic());
        assertEquals("abc", out.get(0).msgId());
        assertEquals("1001", out.get(0).seq());
    }

    @Test
    @DisplayName("status 缺失/为空/非 COMMITTED 一律忽略")
    void nonCommittedIgnored() {
        List<Received> out = new CopyOnWriteArrayList<>();
        MqttClientManager manager = managerWith(out);

        manager.dispatchAck("ship/413999999/ack", message("{\"msg_id\":\"a\",\"seq\":\"1\"}"));
        manager.dispatchAck("ship/413999999/ack", message("{\"msg_id\":\"b\",\"seq\":\"2\",\"status\":\"\"}"));
        manager.dispatchAck("ship/413999999/ack",
                message("{\"msg_id\":\"c\",\"seq\":\"3\",\"status\":\"RECEIVED\"}"));
        manager.dispatchAck("ship/413999999/ack",
                message("{\"msg_id\":\"d\",\"seq\":\"4\",\"status\":\"MYSQL_COMMITTED\"}"));
        manager.dispatchAck("ship/413999999/ack", message("{not-json"));

        assertTrue(out.isEmpty(), "只有 KAFKA_COMMITTED 能进跟踪器");
    }

    @Test
    @DisplayName("无监听器/非 ACK 主题不抛不卡")
    void noListenerOrForeignTopicSafe() {
        MqttClientManager manager = new MqttClientManager(new EdgeProperties());
        manager.dispatchAck("ship/413999999/ack", message(
                "{\"msg_id\":\"a\",\"status\":\"KAFKA_COMMITTED\"}"));
        manager.dispatchAck("zncb/413999999/nmea_gps", message("{}"));
    }
}
