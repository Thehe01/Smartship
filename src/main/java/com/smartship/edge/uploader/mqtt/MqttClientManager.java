package com.smartship.edge.uploader.mqtt;

import com.smartship.edge.config.EdgeProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 边端到岸端 MQTT 客户端生命周期管理器
 * <p>
 * 采用 Paho MQTT 客户端，配置自动重连与 QoS 1 保证至少送达一次
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MqttClientManager {

    private final EdgeProperties properties;
    private MqttClient client;

    @PostConstruct
    public void init() {
        if (properties.getUploader().getMqtt().isEnabled()) {
            connect();
        }
    }

    public synchronized void connect() {
        try {
            disconnect();
            EdgeProperties.MqttConfig mqtt = properties.getUploader().getMqtt();
            String clientId = StringUtils.hasText(mqtt.getClientId())
                    ? mqtt.getClientId()
                    : "smartship_edge_" + System.currentTimeMillis();

            client = new MqttClient(mqtt.getBrokerUrl(), clientId, new MemoryPersistence());

            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(mqtt.isAutoReconnect());
            options.setCleanSession(true);
            options.setKeepAliveInterval(mqtt.getKeepAliveInterval());
            options.setConnectionTimeout(mqtt.getConnectionTimeout());
            if (StringUtils.hasText(mqtt.getUsername())) {
                options.setUserName(mqtt.getUsername());
                options.setPassword(mqtt.getPassword() == null ? new char[0] : mqtt.getPassword().toCharArray());
            }

            client.connect(options);
            log.info("[MQTT] 成功连接至岸端 Broker: {}, ClientId: {}", mqtt.getBrokerUrl(), clientId);
        } catch (Exception e) {
            log.warn("[MQTT] 连接 Broker 失败 (弱网离线，稍后将自动重试): {}", e.getMessage());
        }
    }

    public boolean publish(String topic, byte[] payload) {
        if (!properties.getUploader().getMqtt().isEnabled()) {
            return false;
        }
        if (client == null || !client.isConnected()) {
            connect();
        }
        if (client == null || !client.isConnected()) {
            return false;
        }
        try {
            MqttMessage message = new MqttMessage(payload);
            message.setQos(properties.getUploader().getMqtt().getQos());
            message.setRetained(false);
            client.publish(topic, message);
            return true;
        } catch (MqttException e) {
            log.warn("[MQTT] 发布失败 topic={}, err={}", topic, e.getMessage());
            return false;
        }
    }

    @PreDestroy
    public synchronized void disconnect() {
        try {
            if (client != null) {
                if (client.isConnected()) {
                    client.disconnect();
                }
                client.close();
            }
        } catch (MqttException e) {
            log.warn("[MQTT] 断开连接异常: {}", e.getMessage());
        }
    }
}
