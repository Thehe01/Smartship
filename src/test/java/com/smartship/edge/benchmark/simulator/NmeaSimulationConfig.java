package com.smartship.edge.benchmark.simulator;

import java.time.Duration;

/**
 * NMEA 模拟配置。
 *
 * @param messageRatePerSecond 流式发射速率（条/秒，直接驱动场景可忽略 pacing）
 * @param duration             流式发射时长上限
 * @param mmsi                 模拟船舶 MMSI（仅标注，不编入语句）
 * @param noiseRate            畸形/噪声报文比例（0~1）
 * @param invalidChecksumRate  校验和错误报文比例（0~1）
 * @param seed                 随机种子（固定种子保证可重复）
 */
public record NmeaSimulationConfig(
        int messageRatePerSecond,
        Duration duration,
        String mmsi,
        double noiseRate,
        double invalidChecksumRate,
        long seed) {

    public NmeaSimulationConfig(int messageRatePerSecond, Duration duration, String mmsi,
                                double noiseRate, double invalidChecksumRate) {
        this(messageRatePerSecond, duration, mmsi, noiseRate, invalidChecksumRate, 42L);
    }
}
