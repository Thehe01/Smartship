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

    @Test
    @DisplayName("测试兜底场景下 TreeMap 字典序规范化 JSON 散列的确定性（不同 Map 遍历顺序生成一致指纹）")
    void testFallbackCanonicalSorting() {
        // map1: latitude -> longitude -> speed -> course
        java.util.Map<String, Object> map1 = new java.util.LinkedHashMap<>();
        map1.put("latitude", 31.2304);
        map1.put("longitude", 121.4737);
        map1.put("speed", 14.2);
        map1.put("course", 270.5);

        // map2: course -> speed -> latitude -> longitude (不同插入顺序)
        java.util.Map<String, Object> map2 = new java.util.LinkedHashMap<>();
        map2.put("course", 270.5);
        map2.put("speed", 14.2);
        map2.put("latitude", 31.2304);
        map2.put("longitude", 121.4737);

        // 两者无主键与时间戳，触发 fallback canonical JSON
        String hash1 = MqttPublisher.stableMessageId("413999999", "custom_sensor", map1);
        String hash2 = MqttPublisher.stableMessageId("413999999", "custom_sensor", map2);

        assertNotNull(hash1);
        assertEquals(64, hash1.length());
        assertEquals(hash1, hash2, "无论 Map 插入顺序如何，规范化字典序排序后必须生成 100% 恒定的指纹");

        // 当载荷值变化时，生成不同的指纹
        java.util.Map<String, Object> map3 = new java.util.LinkedHashMap<>(map1);
        map3.put("speed", 14.3);
        String hash3 = MqttPublisher.stableMessageId("413999999", "custom_sensor", map3);
        assertNotEquals(hash1, hash3, "数据内容变化必须生成不同的散列指纹");
    }
}
