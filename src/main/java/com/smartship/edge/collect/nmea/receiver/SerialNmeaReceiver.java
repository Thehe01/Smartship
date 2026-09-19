package com.smartship.edge.collect.nmea.receiver;

import com.fazecast.jSerialComm.SerialPort;
import com.fazecast.jSerialComm.SerialPortDataListener;
import com.fazecast.jSerialComm.SerialPortEvent;
import com.smartship.edge.collect.nmea.service.NmeaDataService;
import com.smartship.edge.config.EdgeProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 物理串口 NMEA 接收器
 * <p>
 * 基于 jSerialComm 库实现跨平台（Windows / Linux / macOS）串口异步事件监听。
 * 内置行缓冲区滑动切分机制，完美解决无边界字节流中的断帧（半包）与粘包。
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "smartship.edge.collect.nmea.serial.enabled", havingValue = "true", matchIfMissing = true)
public class SerialNmeaReceiver {

    private final EdgeProperties properties;
    private final NmeaDataService nmeaDataService;

    private volatile boolean running = false;
    private ExecutorService executor;
    private final List<SerialPort> openedPorts = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void init() {
        EdgeProperties.SerialConfig config = properties.getCollect().getNmea().getSerial();
        if (config.isEnabled()) {
            start();
        }
    }

    public void start() {
        EdgeProperties.SerialConfig config = properties.getCollect().getNmea().getSerial();
        running = true;
        executor = Executors.newCachedThreadPool();

        List<String> portNames = resolvePortNames(config);
        if (portNames.isEmpty()) {
            log.warn("[NMEA-SERIAL] 未检测到可用串口，请检查硬件连接或在 application.yml 中手动指定端口");
            return;
        }

        for (String portName : portNames) {
            executor.submit(() -> openAndListen(portName, config));
        }
        log.info("[NMEA-SERIAL] 串口监听服务已启动，端口列表: {}", portNames);
    }

    private List<String> resolvePortNames(EdgeProperties.SerialConfig config) {
        String rawPort = config.getPort();
        if (rawPort == null || rawPort.trim().isEmpty() || "auto".equalsIgnoreCase(rawPort.trim())) {
            return autoDetectPorts();
        }
        return Arrays.stream(rawPort.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    private List<String> autoDetectPorts() {
        SerialPort[] ports = SerialPort.getCommPorts();
        if (ports == null || ports.length == 0) {
            return List.of();
        }

        List<String> usbPorts = new ArrayList<>();
        List<String> physicalPorts = new ArrayList<>();

        for (SerialPort port : ports) {
            String name = port.getSystemPortName().toLowerCase();
            String desc = port.getPortDescription().toLowerCase();
            if (name.contains("ttyusb") || desc.contains("usb") || desc.contains("ch340") || desc.contains("cp210")) {
                usbPorts.add(port.getSystemPortName());
            } else if (name.contains("tty") || name.startsWith("com")) {
                physicalPorts.add(port.getSystemPortName());
            }
        }
        return !usbPorts.isEmpty() ? usbPorts : physicalPorts;
    }

    private void openAndListen(String portName, EdgeProperties.SerialConfig config) {
        SerialPort serialPort = findPort(portName);
        if (serialPort == null) {
            log.error("[NMEA-SERIAL] 无法找到串口设备 [{}]", portName);
            return;
        }

        serialPort.setComPortParameters(
                config.getBaudRate(),
                config.getDataBits(),
                SerialPort.ONE_STOP_BIT,
                SerialPort.NO_PARITY
        );

        if (!serialPort.openPort()) {
            log.error("[NMEA-SERIAL] 串口 [{}] 打开失败（可能已被占用）", portName);
            return;
        }

        serialPort.setComPortTimeouts(SerialPort.TIMEOUT_NONBLOCKING, 0, 0);
        openedPorts.add(serialPort);
        log.info("[NMEA-SERIAL] 串口 [{}] 已打开，波特率: {}", portName, config.getBaudRate());

        // 注册事件驱动数据监听器
        serialPort.addDataListener(new SerialPortDataListener() {
            private final StringBuilder lineBuffer = new StringBuilder();

            @Override
            public int getListeningEvents() {
                return SerialPort.LISTENING_EVENT_DATA_AVAILABLE;
            }

            @Override
            public void serialEvent(SerialPortEvent event) {
                if (event.getEventType() != SerialPort.LISTENING_EVENT_DATA_AVAILABLE) {
                    return;
                }

                try {
                    byte[] buffer = new byte[4096];
                    int len = serialPort.readBytes(buffer, buffer.length);
                    if (len <= 0) return;

                    // 1. 追加进入专用行缓冲区
                    lineBuffer.append(new String(buffer, 0, len, StandardCharsets.US_ASCII));

                    // 2. 循环按换行符切分完整报文
                    String content = lineBuffer.toString();
                    int splitIndex;
                    while ((splitIndex = findLineEnd(content)) >= 0) {
                        String line = content.substring(0, splitIndex).trim();
                        content = content.substring(splitIndex + getLineEndLength(content, splitIndex));

                        if (!line.isEmpty()) {
                            nmeaDataService.processSentence(line, "SERIAL:" + portName);
                        }
                    }

                    // 3. 截留未闭合的“半包”片段
                    lineBuffer.setLength(0);
                    lineBuffer.append(content);
                } catch (Exception e) {
                    log.error("[NMEA-SERIAL] 串口 [{}] 数据读取异常: {}", portName, e.getMessage());
                }
            }
        });
    }

    private int findLineEnd(String str) {
        int cr = str.indexOf('\r');
        int lf = str.indexOf('\n');
        if (cr >= 0 && lf >= 0) return Math.min(cr, lf);
        if (cr >= 0) return cr;
        return lf;
    }

    private int getLineEndLength(String str, int pos) {
        if (pos + 1 < str.length() && str.charAt(pos) == '\r' && str.charAt(pos + 1) == '\n') {
            return 2;
        }
        return 1;
    }

    private SerialPort findPort(String portName) {
        for (SerialPort port : SerialPort.getCommPorts()) {
            if (port.getSystemPortName().equalsIgnoreCase(portName)) {
                return port;
            }
        }
        return null;
    }

    @PreDestroy
    public void stop() {
        running = false;
        for (SerialPort port : openedPorts) {
            try {
                if (port.isOpen()) port.closePort();
            } catch (Exception ignored) {}
        }
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
        }
        log.info("[NMEA-SERIAL] 串口接收服务已停止");
    }
}
