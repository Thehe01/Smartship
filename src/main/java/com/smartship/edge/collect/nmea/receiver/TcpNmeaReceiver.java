package com.smartship.edge.collect.nmea.receiver;

import com.smartship.edge.collect.nmea.service.NmeaDataService;
import com.smartship.edge.config.EdgeProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "smartship.edge.collect.nmea.tcp.enabled", havingValue = "true")
public class TcpNmeaReceiver {

    private final EdgeProperties properties;
    private final NmeaDataService nmeaDataService;

    private volatile boolean running = false;
    private Thread listenerThread;
    private Socket socket;

    @PostConstruct
    public void start() {
        running = true;
        listenerThread = new Thread(this::listenLoop, "TcpNmeaReceiver-Thread");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void listenLoop() {
        EdgeProperties.TcpConfig config = properties.getCollect().getNmea().getTcp();
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                log.info("[NMEA-TCP] 尝试连接服务端 {}:{}", config.getHost(), config.getPort());
                socket = new Socket(config.getHost(), config.getPort());
                socket.setSoTimeout(10000);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
                String line;
                while (running && (line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        nmeaDataService.processSentence(line.trim(), "TCP:" + config.getHost() + ":" + config.getPort());
                    }
                }
            } catch (Exception e) {
                log.warn("[NMEA-TCP] 连接异常断开: {}，5秒后尝试重连", e.getMessage());
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } finally {
                closeSocket();
            }
        }
    }

    private void closeSocket() {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (Exception ignored) {}
            socket = null;
        }
    }

    @PreDestroy
    public void stop() {
        running = false;
        closeSocket();
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
    }
}
