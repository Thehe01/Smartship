package com.smartship.edge;

import com.smartship.edge.collect.nmea.parser.NmeaParser;
import com.smartship.edge.collect.nmea.service.NmeaDataHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NmeaChecksumTest {

    @Test
    @DisplayName("测试 NMEA 0183 异或校验算法")
    void testValidateChecksum() {
        // 标准 RMC 语句：末尾 *51 为计算所得正确的十六进制校验和
        String validSentence = "$GPRMC,083559.00,A,4717.11437,N,00833.91522,E,0.004,77.52,091204,,,A*51";
        assertTrue(NmeaParser.validateChecksum(validSentence), "合法的 NMEA 校验和应通过");

        // 篡改一个字符模拟串口电磁干扰误码
        String corruptedSentence = "$GPRMC,083559.00,A,4717.11438,N,00833.91522,E,0.004,77.52,091204,,,A*51";
        assertFalse(NmeaParser.validateChecksum(corruptedSentence), "被篡改的误码 NMEA 语句应被拦截丢弃");

        // 格式不全或非法校验码测试
        assertFalse(NmeaParser.validateChecksum("$GP*ZZ"));
        assertFalse(NmeaParser.validateChecksum(""));
        assertFalse(NmeaParser.validateChecksum(null));
    }

    @Test
    @DisplayName("测试航海度分(ddmm.mmmm)到十进制度数转换")
    void testParseNmeaCoord() {
        // 纬度测试: 3112.3456, N -> 31 + 12.3456/60 = 31.20576
        double lat = NmeaDataHandler.parseNmeaCoord("3112.3456", "N", false);
        assertEquals(31.20576, lat, 0.0001);

        // 南纬测试: 3112.3456, S -> -31.20576
        double southLat = NmeaDataHandler.parseNmeaCoord("3112.3456", "S", false);
        assertEquals(-31.20576, southLat, 0.0001);

        // 经度测试: 12130.6000, E -> 121 + 30.6/60 = 121.51
        double lon = NmeaDataHandler.parseNmeaCoord("12130.6000", "E", true);
        assertEquals(121.51, lon, 0.0001);

        // 西经测试: 12130.6000, W -> -121.51
        double westLon = NmeaDataHandler.parseNmeaCoord("12130.6000", "W", true);
        assertEquals(-121.51, westLon, 0.0001);
    }
}
