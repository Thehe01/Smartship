package com.smartship.edge.collect.nmea.parser;

import lombok.extern.slf4j.Slf4j;

/**
 * AIS 6-bit payload 解码器
 * <p>
 * 符合 ITU-R M.1371 国际标准。
 * 将 !AIVDO / !AIVDM 消息中的 6-bit ASCII armoring 字段解码为二进制流，提取船舶 MMSI。
 * 当前支持 Type 1/2/3（Class A 位置报告）及 Type 18/19（Class B）。
 *
 * @author SmartShip Architecture Team
 */
@Slf4j
public final class AisPayloadDecoder {

    private AisPayloadDecoder() {
    }

    /**
     * 从 AIS 6-bit payload 中解码 9 位 MMSI
     *
     * @param payload  AIS payload 字符串（6-bit 编码）
     * @param fillBits 填充位数（NMEA 语句末尾字段）
     * @return MMSI 字符串（9 位数字），解码失败返回 null
     */
    public static String decodeMmsi(String payload, int fillBits) {
        if (payload == null || payload.isEmpty()) {
            log.warn("[AIS] payload 为空");
            return null;
        }

        // 1. 将 6-bit 字符逐个逆向映射为二进制比特串
        StringBuilder bits = new StringBuilder();
        for (int i = 0; i < payload.length(); i++) {
            int value = ais6bitToValue(payload.charAt(i));
            if (value < 0) {
                log.warn("[AIS] payload 包含无效字符: '{}' (0x{})", payload.charAt(i),
                        Integer.toHexString(payload.charAt(i)));
                return null;
            }
            bits.append(String.format("%6s", Integer.toBinaryString(value)).replace(' ', '0'));
        }

        // 2. 剥离无用的尾部填充位
        if (fillBits > 0 && fillBits < 6 && bits.length() > fillBits) {
            bits.setLength(bits.length() - fillBits);
        }

        // 3. 提取消息类型 (bits 0-5)
        if (bits.length() < 6) {
            log.warn("[AIS] payload 过短，无法提取消息类型");
            return null;
        }
        int messageType = Integer.parseInt(bits.substring(0, 6), 2);

        // 4. 支持 Class A (Type 1/2/3) 及 Class B (Type 18/19) 位置报告
        if (messageType != 1 && messageType != 2 && messageType != 3 && messageType != 18 && messageType != 19) {
            log.debug("[AIS] 不支持的消息类型: {}, 跳过 MMSI 解析", messageType);
            return null;
        }

        // 5. 提取 MMSI (bits 8-37 共 30 bits)
        if (bits.length() < 38) {
            log.warn("[AIS] payload 比特长度不足 38 bits (实际: {})，无法提取 MMSI", bits.length());
            return null;
        }
        String mmsiBits = bits.substring(8, 38);
        long mmsiLong = Long.parseLong(mmsiBits, 2);

        // 6. 格式化为标准 9 位数字字符（不足前置补 0）
        String mmsi = String.format("%09d", mmsiLong);
        log.info("[AIS] 成功解码 Type{} 位置报文，本船 MMSI: {}", messageType, mmsi);
        return mmsi;
    }

    /**
     * AIS 6-bit 字符转数值（数学逆映射）
     * 规则：
     * ASCII 48('0') ~ 87('W')  -> 减去 48 (0 ~ 39)
     * ASCII 96('`') ~ 119('w') -> 减去 56 (40 ~ 63)
     */
    public static int ais6bitToValue(char c) {
        int code = (int) c;
        if (code >= 48 && code <= 87) {
            return code - 48;
        } else if (code >= 96 && code <= 119) {
            return code - 56;
        }
        return -1;
    }
}
