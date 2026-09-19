package com.smartship.edge.uploader.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * MQTT 消息发布与确定性业务指纹计算器
 * <p>
 * 核心架构特性：
 * 1. 确定性稳定消息 ID（msg_id）：以业务自然主键与时间戳绑定计算 SHA-256 哈希，杜绝随机 UUID，为岸端提供天然幂等去重基石
 * 2. 统一上报主题规范：zncb/{mmsi}/{topicSuffix}
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MqttPublisher {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    private final MqttClientManager mqttClientManager;

    public boolean publish(String mmsi, String type, String topicSuffix, Map<String, Object> row) {
        Map<String, Object> payload = new LinkedHashMap<>(row);
        payload.put("mmsi", mmsi);
        payload.put("type", type);
        payload.put("msg_id", stableMessageId(mmsi, type, row));
        payload.put("timestamp", ZonedDateTime.now(ZoneOffset.ofHours(8)).format(TIME_FMT));

        try {
            String topic = "zncb/" + mmsi + "/" + topicSuffix;
            return mqttClientManager.publish(topic, MAPPER.writeValueAsBytes(payload));
        } catch (Exception e) {
            log.warn("[MQTT-Pub] 序列化/发送失败: type={}, mmsi={}, err={}", type, mmsi, e.getMessage());
            return false;
        }
    }

    /**
     * 计算确定性稳定 Message ID（SHA-256 散列）
     */
    public static String stableMessageId(String mmsi, String type, Map<String, Object> row) {
        String sourceIdentity = first(row, "source_id", "external_alarm_id", "local_id", "id", "device_code");
        String sourceTime = first(row, "update_time", "updated_at", "rec_time", "alarm_time", "time", "timestamp", "create_time");
        
        String canonical;
        if ("unknown".equals(sourceIdentity) && "unknown".equals(sourceTime)) {
            // 兜底防撞：若无显式主键与时间戳，以有序载荷内容为指纹基准，杜绝不同快照碰撞同一 msg_id
            canonical = mmsi + "|" + type + "|payload:" + row;
        } else {
            canonical = mmsi + "|" + type + "|" + sourceIdentity + "|" + sourceTime;
        }

        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception ex) {
            throw new IllegalStateException("无法生成稳定 MQTT 消息 ID: " + ex.getMessage(), ex);
        }
    }

    private static String first(Map<String, Object> row, String... keys) {
        for (String key : keys) {
            Object value = row.get(key);
            if (value != null && !String.valueOf(value).isBlank()) {
                return String.valueOf(value);
            }
        }
        return "unknown";
    }
}
