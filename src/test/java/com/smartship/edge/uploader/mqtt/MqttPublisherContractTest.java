package com.smartship.edge.uploader.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * 线路协议契约 <b>edge-mqtt-contract-v1</b> 的本仓冻结端。
 *
 * <p>与岸仓 {@code EdgeContractPinTest}  pin 住同一组向量：主题格式、msg_id 规范算法、
 * 载荷必填键。任何一侧改动导致本测试变红时，必须同步 bump 另一侧——单边绿不代表兼容。
 */
class MqttPublisherContractTest {

    /** 冻结向量 V1：SHA-256("413999999|nmea_gps|1001|2026-09-19T10:00:00")。 */
    static final String FROZEN_MSG_ID_V1 =
            "7b20b18ae89960f8f6cd04a198af268de85b67ca72e3adc4b7cfd224c960bc69";
    static final String FROZEN_TOPIC_V1 = "zncb/413999999/nmea_gps";

    private static Map<String, Object> v1Row() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1001L);
        row.put("update_time", "2026-09-19T10:00:00");
        row.put("speed", 12.5);
        return row;
    }

    @Test
    @DisplayName("契约v1：自然主键行 msg_id 冻结，改算法必须同步 bump 岸仓")
    void frozenStableIdVector() {
        assertEquals(FROZEN_MSG_ID_V1,
                MqttPublisher.stableMessageId("413999999", "nmea_gps", v1Row()));
    }

    @Test
    @DisplayName("契约v1：主题格式 zncb/{mmsi}/{suffix}")
    void frozenTopicFormat() {
        assertEquals(FROZEN_TOPIC_V1, MqttPublisher.topicFor("413999999", "nmea_gps"));
    }

    @Test
    @DisplayName("契约v1：发布载荷携带 mmsi/type/msg_id/sent_at 且 topic 精确")
    void publishedPayloadCarriesContractKeys() throws Exception {        MqttClientManager manager = mock(MqttClientManager.class);
        when(manager.publish(anyString(), any())).thenReturn(true);
        MqttPublisher publisher = new MqttPublisher(manager);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        publisher.publish("413999999", "nmea_gps", "nmea_gps", v1Row());

        org.mockito.Mockito.verify(manager).publish(topicCaptor.capture(), payloadCaptor.capture());
        assertEquals(FROZEN_TOPIC_V1, topicCaptor.getValue());

        @SuppressWarnings("unchecked")
        Map<String, Object> payload =
                new ObjectMapper().readValue(payloadCaptor.getValue(), Map.class);
        assertEquals("413999999", payload.get("mmsi"));
        assertEquals("nmea_gps", payload.get("type"));
        assertEquals(FROZEN_MSG_ID_V1, payload.get("msg_id"));
        assertTrue(payload.containsKey("sent_at"), "载荷必须携带 sent_at");
    }

    @Test
    @DisplayName("契约v1+replay_id：同 replay_id 不同自增 id → 同 msg_id（回放幂等基石）")
    void replayIdPreferredOverAutoId() {
        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("replay_id", "replay-uuid-1");
        row1.put("id", 100L);
        row1.put("update_time", "2026-09-19T10:00:00");
        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("replay_id", "replay-uuid-1");
        row2.put("id", 101L);
        row2.put("update_time", "2026-09-19T10:00:00");

        assertEquals(MqttPublisher.stableMessageId("413999999", "nmea_gps", row1),
                MqttPublisher.stableMessageId("413999999", "nmea_gps", row2),
                "回放行自增 id 不同但 replay_id 相同，msg_id 必须相同，岸端 UNIQUE 去重");

        Map<String, Object> row3 = new LinkedHashMap<>(row2);
        row3.remove("replay_id");
        assertNotEquals(MqttPublisher.stableMessageId("413999999", "nmea_gps", row1),
                MqttPublisher.stableMessageId("413999999", "nmea_gps", row3),
                "无 replay_id 的行走旧语义（回放行与正常行互不干扰）");
    }
}
