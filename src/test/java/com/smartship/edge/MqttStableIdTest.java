package com.smartship.edge;

import com.smartship.edge.uploader.mqtt.MqttPublisher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MqttStableIdTest {

    @Test
    @DisplayName("测试稳定 Message ID 的确定性与幂等散列")
    void testStableMessageId() {
        Map<String, Object> row = Map.of(
                "id", 10086L,
                "update_time", "2026-09-19 13:00:00",
                "speed", 12.5
        );

        String id1 = MqttPublisher.stableMessageId("413999999", "nmea_gps", row);
        String id2 = MqttPublisher.stableMessageId("413999999", "nmea_gps", row);

        assertNotNull(id1);
        assertEquals(64, id1.length(), "SHA-256 应生成 64 位十六进制散列字符串");
        assertEquals(id1, id2, "相同业务内容在弱网重传时应生成完全恒定一致的 msg_id (幂等基石)");

        // 当业务键改变时，生成不同的 ID
        Map<String, Object> rowDifferentTime = Map.of(
                "id", 10086L,
                "update_time", "2026-09-19 13:00:01",
                "speed", 12.5
        );
        String id3 = MqttPublisher.stableMessageId("413999999", "nmea_gps", rowDifferentTime);
        assertNotEquals(id1, id3, "不同时间戳产生的数据应拥有唯一的散列指纹");
    }
}
