package com.smartship.edge.uploader.mqtt;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.observability.SmartShipMetrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 边端到岸端 MQTT 客户端生命周期管理器
 * <p>
 * 采用 Paho MQTT 客户端，配置自动重连与 QoS 1 保证至少送达一次。
 * 集成 Micrometer 指标暴露连接状态 Gauge、连接/重连/断连 Counter 及发布性能 Timer。
 */
@Slf4j
@Component
public class MqttClientManager implements MeterBinder {

    private final EdgeProperties properties;
    private final SmartShipMetrics metrics;
    private MqttClient client;

    /** Application ACK 监听器：岸端 {@code ship/{mmsi}/ack} 到达时触发。 */
    public interface AckListener {
        void onAck(String topic, String msgId, String seq);
    }

    private volatile AckListener ackListener;
    /** 岸端 Application ACK 的唯一合法状态：Kafka durable 落定。 */
    static final String STATUS_KAFKA_COMMITTED = "KAFKA_COMMITTED";
    /** 已登记 ACK 订阅的 MMSI：cleanSession 下每次（重）连都要重新订阅。 */
    private final Set<String> ackMmsis = ConcurrentHashMap.newKeySet();
    private static final com.fasterxml.jackson.databind.ObjectMapper ACK_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    @Autowired
    public MqttClientManager(EdgeProperties properties, SmartShipMetrics metrics) {
        this.properties = properties;
        this.metrics = metrics;
    }

    public MqttClientManager(EdgeProperties properties) {
        this(properties, null);
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("smartship_mqtt_connected", this, mgr -> mgr.isConnected() ? 1.0 : 0.0)
                .description("MQTT connection status (1 = connected, 0 = disconnected)")
                .register(registry);
    }

    public boolean isConnected() {
        try {
            return client != null && client.isConnected();
        } catch (Exception e) {
            return false;
        }
    }

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

            client.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    if (reconnect) {
                        if (metrics != null) {
                            metrics.recordMqttReconnect();
                        }
                        log.info("[MQTT] 自动重连成功: uri={}", serverURI);
                    }
                    // cleanSession 会话不持久：每次连接都要重建 ACK 订阅。
                    resubscribeAcks();
                }

                @Override
                public void connectionLost(Throwable cause) {
                    if (metrics != null) {
                        metrics.recordMqttConnectionLost();
                    }
                    log.warn("[MQTT] 连接中断: {}", cause != null ? cause.getMessage() : "unknown");
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    dispatchAck(topic, message);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            client.connect(options);
            resubscribeAcks();
            if (metrics != null) {
                metrics.recordMqttConnect(true);
            }
            log.info("[MQTT] 成功连接至岸端 Broker: {}, ClientId: {}", mqtt.getBrokerUrl(), clientId);
        } catch (Exception e) {
            if (metrics != null) {
                metrics.recordMqttConnect(false);
            }
            log.warn("[MQTT] 连接 Broker 失败 (弱网离线，稍后将自动重试): {}", e.getMessage());
        }
    }

    /**
     * 注册 Application ACK 监听器（通常指向上传跟踪器）。
     */
    public void setAckListener(AckListener ackListener) {
        this.ackListener = ackListener;
    }

    /**
     * 确保订阅该 MMSI 的 Application ACK（{@code ship/{mmsi}/ack}，QoS1）。
     * 幂等：重复登记只订一次；离线时仅登记，连接恢复后自动补订。
     */
    public void ensureAckSubscription(String mmsi) {
        if (!StringUtils.hasText(mmsi)) {
            return;
        }
        ackMmsis.add(mmsi);
        MqttClient c = client;
        if (c != null && c.isConnected()) {
            try {
                c.subscribe("ship/" + mmsi + "/ack", properties.getUploader().getMqtt().getQos());
            } catch (Exception e) {
                log.warn("[MQTT] ACK 订阅失败 mmsi={}, err={}", mmsi, e.getMessage());
            }
        }
    }

    private void resubscribeAcks() {
        MqttClient c = client;
        if (c == null || !c.isConnected() || ackMmsis.isEmpty()) {
            return;
        }
        for (String mmsi : ackMmsis) {
            try {
                c.subscribe("ship/" + mmsi + "/ack", properties.getUploader().getMqtt().getQos());
            } catch (Exception e) {
                log.warn("[MQTT] ACK 重订阅失败 mmsi={}, err={}", mmsi, e.getMessage());
            }
        }
    }

    /** 解析岸端 Application ACK 并分发；格式不对的直接忽略（不抛、不卡回调线程）。 */
    void dispatchAck(String topic, MqttMessage message) {
        AckListener listener = ackListener;
        if (listener == null || topic == null || !topic.startsWith("ship/")) {
            return;
        }
        try {
            Map<?, ?> ack = ACK_MAPPER.readValue(message.getPayload(), Map.class);
            Object msgId = ack.get("msg_id");
            if (msgId == null || msgId.toString().isBlank()) {
                return;
            }
            // 只认 Kafka 落定确认：缺失、为空、非 KAFKA_COMMITTED 一律忽略。
            // 未来若有新 status（如消费落定），必须显式加白，不能默认放行。
            Object status = ack.get("status");
            if (!STATUS_KAFKA_COMMITTED.equals(status == null ? null : status.toString())) {
                log.debug("[MQTT] 非 KAFKA_COMMITTED ACK 忽略: topic={}", topic);
                return;
            }
            Object seq = ack.get("seq");
            listener.onAck(topic, msgId.toString(), seq == null ? null : seq.toString());
        } catch (Exception e) {
            log.debug("[MQTT] 非 ACK 消息忽略: topic={}", topic);
        }
    }

    public boolean publish(String topic, byte[] payload) {
        if (!properties.getUploader().getMqtt().isEnabled()) {
            return false;
        }
        long startNanos = System.nanoTime();
        if (client == null || !client.isConnected()) {
            connect();
        }
        if (client == null || !client.isConnected()) {
            if (metrics != null) {
                metrics.recordMqttPublish(false, System.nanoTime() - startNanos);
            }
            return false;
        }
        try {
            MqttMessage message = new MqttMessage(payload);
            message.setQos(properties.getUploader().getMqtt().getQos());
            message.setRetained(false);
            client.publish(topic, message);
            if (metrics != null) {
                metrics.recordMqttPublish(true, System.nanoTime() - startNanos);
            }
            return true;
        } catch (Exception e) {
            if (metrics != null) {
                metrics.recordMqttPublish(false, System.nanoTime() - startNanos);
            }
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
