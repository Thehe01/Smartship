package com.smartship.edge.uploader.mqtt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class MqttTimestampAndStableIdTest {

    private MqttClientManager clientManager;
    private MqttPublisher publisher;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        clientManager = mock(MqttClientManager.class);
        publisher = new MqttPublisher(clientManager);
    }

    @Test
    @DisplayName("Test 1: 原始 row 包含 timestamp 时予以严格保留，绝不被当前时间覆盖，且新增 sent_at")
    void testOriginalTimestampPreservedAndSentAtAdded() throws Exception {
        when(clientManager.publish(any(), any())).thenReturn(true);

        Map<String, Object> row = new HashMap<>();
        row.put("id", 1001L);
        row.put("timestamp", "2026-09-19T10:00:00");
        row.put("speed", 15.5);

        boolean ok = publisher.publish("413999999", "nmea_gps", "gps", row);
        assertTrue(ok);

        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(clientManager).publish(eq("zncb/413999999/gps"), payloadCaptor.capture());

        Map<String, Object> payload = objectMapper.readValue(payloadCaptor.getValue(), new TypeReference<>() {});

        // 验证原 timestamp 绝不被当前时间覆盖
        assertEquals("2026-09-19T10:00:00", payload.get("timestamp"), "原始采集/业务事件时间必须完整保留");

        // 验证必须新增 sent_at 字段记录实际网络发送时间
        assertNotNull(payload.get("sent_at"), "必须新增 sent_at 字段记录实际上传时间");
        assertTrue(payload.get("sent_at").toString().contains("T"), "sent_at 必须为符合 ISO-8601 格式的时间戳");

        // 验证 msg_id 正常生成
        assertNotNull(payload.get("msg_id"));
        assertEquals("413999999", payload.get("mmsi"));
        assertEquals("nmea_gps", payload.get("type"));
    }

    @Test
    @DisplayName("Test 2: 相同 row 重传两次，sent_at 不同但 msg_id 相同，证明发送时间不参与稳定消息 ID")
    void testRetransmissionHasDifferentSentAtButIdenticalMsgId() throws Exception {
        when(clientManager.publish(any(), any())).thenReturn(true);

        Map<String, Object> row = new HashMap<>();
        row.put("id", 2002L);
        row.put("timestamp", "2026-09-19T10:00:00");
        row.put("heading", 185.0);

        // 第一次发送
        publisher.publish("413999999", "nmea_gps", "gps", row);

        // 模拟时间流逝（让系统时钟至少前进 2 毫秒）
        Thread.sleep(10);

        // 第二次重传相同数据行
        publisher.publish("413999999", "nmea_gps", "gps", row);

        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(clientManager, times(2)).publish(eq("zncb/413999999/gps"), payloadCaptor.capture());

        var capturedPayloads = payloadCaptor.getAllValues();
        Map<String, Object> payload1 = objectMapper.readValue(capturedPayloads.get(0), new TypeReference<>() {});
        Map<String, Object> payload2 = objectMapper.readValue(capturedPayloads.get(1), new TypeReference<>() {});

        // 验证两次重传的 msg_id 100% 恒定相同（岸端幂等去重基石）
        assertEquals(payload1.get("msg_id"), payload2.get("msg_id"), "重传消息的 msg_id 必须绝对恒定");

        // 验证两次 sent_at 记录了不同的实际网络发送时间
        assertNotEquals(payload1.get("sent_at"), payload2.get("sent_at"), "两次不同批次发送的 sent_at 应当不同");

        // 验证原始业务事件时间完全相同
        assertEquals(payload1.get("timestamp"), payload2.get("timestamp"));
    }

    @Test
    @DisplayName("Test 3: row 本身不含 timestamp 时不伪造业务时间，仅填充 sent_at")
    void testRowWithoutTimestampDoesNotFabricateEventTime() throws Exception {
        when(clientManager.publish(any(), any())).thenReturn(true);

        Map<String, Object> row = new HashMap<>();
        row.put("id", 3003L);
        row.put("rpm", 1200.0);

        publisher.publish("413999999", "modbus_engine", "engine", row);

        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(clientManager).publish(eq("zncb/413999999/engine"), payloadCaptor.capture());

        Map<String, Object> payload = objectMapper.readValue(payloadCaptor.getValue(), new TypeReference<>() {});

        assertNull(payload.get("timestamp"), "row 无 timestamp 时不伪造业务时间");
        assertNotNull(payload.get("sent_at"), "sent_at 必须正常填充");
        assertNotNull(payload.get("msg_id"), "msg_id 必须正常生成");
    }
}
