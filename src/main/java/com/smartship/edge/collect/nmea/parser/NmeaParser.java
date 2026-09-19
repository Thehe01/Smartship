package com.smartship.edge.collect.nmea.parser;

import com.smartship.edge.collect.nmea.service.NmeaDataHandler;
import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.config.MmsiPersistence;
import com.smartship.edge.routing.ShipAutoRegisterService;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import com.smartship.edge.uploader.mqtt.MqttClientManager;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * NMEA 0183 协议解析分发器
 * <p>
 * 核心职责：
 * 1. NMEA 逐字节异或和（XOR Checksum）严格校验
 * 2. Talker ID 与语句类型提取分发
 * 3. AIS !AIVDO 报文动态识别本船身份，并驱动“连续 3 帧确认防抖”状态机
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NmeaParser {

    private final NmeaDataHandler dataHandler;
    private final NmeaDataPersistenceService persistenceService;
    private final EdgeProperties properties;
    private final ShipAutoRegisterService shipAutoRegisterService;
    private final MqttClientManager mqttClientManager;

    private String staticConfiguredMmsi;
    private final AtomicInteger pendingMmsiConfirmCount = new AtomicInteger(0);
    private String pendingMmsi = null;
    private static final int REQUIRED_MMSI_CONFIRMATIONS = 3;

    @PostConstruct
    public void init() {
        if (properties.getMmsi() != null && !properties.getMmsi().isBlank()) {
            staticConfiguredMmsi = properties.getMmsi().trim();
            log.info("[NMEA] 静态 MMSI 配置锁定: {}", staticConfiguredMmsi);
        } else {
            // 尝试读取上次断电前持久化的本船 MMSI
            String cached = MmsiPersistence.read();
            if (cached != null && isValidMmsi(cached)) {
                log.info("[NMEA] 从本地缓存恢复 MMSI: {}", cached);
                properties.setMmsi(cached);
                shipAutoRegisterService.ensureRegistered(cached);
            }
        }
    }

    /**
     * 逐字节异或校验和计算
     * 算法：从起始符 ($ 或 !) 之后的第一位到 * 之间的所有字符执行 XOR 运算
     */
    public static boolean validateChecksum(String sentence) {
        if (sentence == null || sentence.length() < 4) {
            return false;
        }
        int starIdx = sentence.lastIndexOf('*');
        if (starIdx < 0) {
            return true; // 兼容无校验的老旧设备
        }
        if (starIdx + 2 >= sentence.length()) {
            return false;
        }
        int startIdx = (sentence.startsWith("$") || sentence.startsWith("!")) ? 1 : 0;
        int calculated = 0;
        for (int i = startIdx; i < starIdx; i++) {
            calculated ^= sentence.charAt(i);
        }
        String hex = sentence.substring(starIdx + 1, starIdx + 3);
        try {
            int expected = Integer.parseInt(hex, 16);
            return calculated == expected;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public void parse(String sentence, String source) {
        if (sentence == null || sentence.trim().isEmpty()) {
            return;
        }

        String clean = sentence.trim();

        // 1. 异或校验
        if (!validateChecksum(clean)) {
            log.warn("[NMEA] 异或校验失败，丢弃误码语句: {}", clean);
            return;
        }

        String talkerId = "";
        String type = "";

        try {
            if ((clean.startsWith("$") || clean.startsWith("!")) && clean.length() >= 6) {
                String name = clean.substring(1, 6);
                talkerId = name.substring(0, 2);
                type = name.substring(2);
            }
        } catch (Exception e) {
            log.warn("[NMEA] 报文头部截取异常: {}", clean);
            return;
        }
        if (type.isEmpty()) {
            return;
        }

        // 2. AIS 本船动态报告识别
        if ("VDO".equals(type)) {
            String decodedMmsi = decodeAisMmsi(clean);
            if (decodedMmsi != null && isValidMmsi(decodedMmsi)) {
                if (staticConfiguredMmsi != null) {
                    if (!staticConfiguredMmsi.equals(decodedMmsi)) {
                        log.warn("[NMEA] 收到动态 MMSI {}，但静态配置锁定为 {}，丢弃动态覆盖",
                                decodedMmsi, staticConfiguredMmsi);
                    }
                } else {
                    handleDynamicMmsi(decodedMmsi);
                }
            }
        }

        String mmsi = properties.getMmsi();
        if (mmsi == null || mmsi.isEmpty()) {
            log.debug("[NMEA] 本船 MMSI 尚未就绪，跳过入库: type={}, source={}", type, source);
            return;
        }

        // 3. 业务数据分发至聚合处理器
        try {
            switch (type) {
                case "GGA" -> dataHandler.handleGGA(clean, source, mmsi);
                case "RMC" -> dataHandler.handleRMC(clean, source, mmsi);
                case "VTG" -> dataHandler.handleVTG(clean, source, mmsi);
                case "HDT" -> dataHandler.handleHDT(clean, source, mmsi);
                case "HDG" -> dataHandler.handleHDG(clean, source, mmsi);
                case "DBT" -> dataHandler.handleDBT(clean, source, mmsi);
                case "DPT" -> dataHandler.handleDPT(clean, source, mmsi);
                case "MWV" -> dataHandler.handleMWV(clean, source, mmsi);
                case "MWD" -> dataHandler.handleMWD(clean, source, mmsi);
                case "RSA" -> dataHandler.handleRSA(clean, source, mmsi);
                default -> log.debug("[NMEA] 未处理类型: {}", type);
            }
        } catch (Exception e) {
            log.warn("[NMEA] 处理器执行异常: type={}, err={}", type, e.getMessage());
        }
    }

    private String decodeAisMmsi(String sentence) {
        try {
            String[] fields = sentence.split(",");
            if (fields.length < 7) {
                return null;
            }
            String payload = fields[5];
            String fillStr = fields[6].replaceAll("[*].*$", "").trim();
            int fillBits = Integer.parseInt(fillStr);
            return AisPayloadDecoder.decodeMmsi(payload, fillBits);
        } catch (Exception e) {
            log.warn("[NMEA] AIS MMSI 解码失败: {}", e.getMessage());
            return null;
        }
    }

    private boolean isValidMmsi(String mmsi) {
        return mmsi != null && mmsi.matches("\\d{9}") && !mmsi.equals("000000000");
    }

    private void handleDynamicMmsi(String decodedMmsi) {
        String currentMmsi = properties.getMmsi();
        if (currentMmsi == null || currentMmsi.isEmpty()) {
            properties.setMmsi(decodedMmsi);
            log.info("[NMEA] 首次动态发现本船 MMSI: {}", decodedMmsi);
            shipAutoRegisterService.ensureRegistered(decodedMmsi);
        } else if (!currentMmsi.equals(decodedMmsi)) {
            // 连续 3 帧确认防抖机制
            if (decodedMmsi.equals(pendingMmsi)) {
                if (pendingMmsiConfirmCount.incrementAndGet() >= REQUIRED_MMSI_CONFIRMATIONS) {
                    log.warn("[NMEA] 连续 {} 帧确认本船 MMSI 发生切换: {} -> {}",
                            REQUIRED_MMSI_CONFIRMATIONS, currentMmsi, decodedMmsi);
                    dataHandler.clearCaches(); // 清空旧船残留缓存，杜绝串库
                    properties.setMmsi(decodedMmsi);
                    MmsiPersistence.write(decodedMmsi);
                    shipAutoRegisterService.ensureRegistered(decodedMmsi);
                    pendingMmsi = null;
                    pendingMmsiConfirmCount.set(0);
                }
            } else {
                pendingMmsi = decodedMmsi;
                pendingMmsiConfirmCount.set(1);
            }
        } else {
            pendingMmsi = null;
            pendingMmsiConfirmCount.set(0);
        }
    }
}
