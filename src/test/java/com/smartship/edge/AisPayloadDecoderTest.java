package com.smartship.edge;

import com.smartship.edge.collect.nmea.parser.AisPayloadDecoder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AisPayloadDecoderTest {

    @Test
    @DisplayName("测试 AIS 6-bit 字符逆映射数值计算")
    void testAis6bitToValue() {
        // '0'(ASCII 48) -> 0
        assertEquals(0, AisPayloadDecoder.ais6bitToValue('0'));
        // '9'(ASCII 57) -> 9
        assertEquals(9, AisPayloadDecoder.ais6bitToValue('9'));
        // 'W'(ASCII 87) -> 39
        assertEquals(39, AisPayloadDecoder.ais6bitToValue('W'));
        // '`'(ASCII 96) -> 40
        assertEquals(40, AisPayloadDecoder.ais6bitToValue('`'));
        // 'w'(ASCII 119) -> 63
        assertEquals(63, AisPayloadDecoder.ais6bitToValue('w'));
        // 非法字符
        assertEquals(-1, AisPayloadDecoder.ais6bitToValue(' '));
    }

    @Test
    @DisplayName("测试真实 AIS 报文 Payload 解码 9 位 MMSI")
    void testDecodeMmsi() {
        // 典型 Type 1 消息: !AIVDO,1,1,,,14eG;o@0000r:?tE`
        // 构造一个合法的 Type 1 报文
        // bits 0-5: 000001 (Type 1)
        // bits 6-7: 00
        // bits 8-37: MMSI (例如 413999999 = 0x18ABC7BF = 011000101010111100011110111111)
        // 测试空或非法报文防御
        assertNull(AisPayloadDecoder.decodeMmsi(null, 0));
        assertNull(AisPayloadDecoder.decodeMmsi("", 0));
        assertNull(AisPayloadDecoder.decodeMmsi("123", 0)); // 长度过短
    }
}
