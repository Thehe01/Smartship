package com.smartship.edge.collect.nmea.receiver;

import com.smartship.edge.collect.nmea.service.NmeaDataService;
import com.smartship.edge.config.EdgeProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "smartship.edge.collect.nmea.udp.enabled", havingValue = "true")
public class UdpNmeaReceiver {

    private final EdgeProperties properties;
    private final NmeaDataService nmeaDataService;

    private volatile boolean running = false;
    private DatagramSocket socket;
    private Thread listenerThread;

    @PostConstruct
    public void start() {
        running = true;
        listenerThread = new Thread(this::listenLoop, "UdpNmeaReceiver-Thread");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void listenLoop() {
        int port = properties.getCollect().getNmea().getUdp().getPort();
        try {
            socket = new DatagramSocket(port);
            log.info("[NMEA-UDP] 已绑定并开始监听端口: {}", port);
            byte[] buffer = new byte[2048];
            while (running && !Thread.currentThread().isInterrupted()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String sentence = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.US_ASCII).trim();
                for (String line : sentence.split("\r?\n")) {
                    if (!line.trim().isEmpty()) {
                        nmeaDataService.processSentence(line.trim(), "UDP:" + port);
                    }
                }
            }
        } catch (Exception e) {
            if (running) {
                log.error("[NMEA-UDP] 监听异常: {}", e.getMessage());
            }
        }
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
    }
}
