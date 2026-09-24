package com.smartship.edge.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "smartship.edge")
public class EdgeProperties {

    private String mmsi = "";
    private String shipId = "";
    private volatile boolean schemaReady = false;

    private Collect collect = new Collect();
    private Uploader uploader = new Uploader();

    @Data
    public static class Collect {
        private Persist persist = new Persist();
        private Nmea nmea = new Nmea();
    }

    @Data
    public static class Persist {
        private boolean enabled = true;
        private int minWriteIntervalSeconds = 1;
        /** 主机工况批量写入开关：开启后 Modbus 采集线程按船攒批，满批后一次 batchUpdate 落库。默认关闭，行为与单行 saveEngine 完全一致。 */
        private boolean engineBatchEnabled = false;
        /** 满批行数（按船分组计数），默认 500。 */
        private int engineBatchSize = 500;
        private PoolConfig pool = new PoolConfig();
    }

    @Data
    public static class PoolConfig {
        private int coreSize = 2;
        private int maxSize = 4;
        private int queueCapacity = 500;
        private int keepAliveSeconds = 60;
        private int awaitTerminationSeconds = 30;
    }

    @Data
    public static class Nmea {
        private SerialConfig serial = new SerialConfig();
        private TcpConfig tcp = new TcpConfig();
        private UdpConfig udp = new UdpConfig();
    }

    @Data
    public static class SerialConfig {
        private boolean enabled = true;
        private String port = "auto";
        private int baudRate = 9600;
        private int dataBits = 8;
        private int stopBits = 1;
        private String parity = "none";
    }

    @Data
    public static class TcpConfig {
        private boolean enabled = false;
        private String host = "127.0.0.1";
        private int port = 9001;
    }

    @Data
    public static class UdpConfig {
        private boolean enabled = false;
        private int port = 9002;
    }

    @Data
    public static class Uploader {
        private boolean enabled = true;
        private PollConfig poll = new PollConfig();
        private MqttConfig mqtt = new MqttConfig();
        private AckConfig ack = new AckConfig();
    }

    /**
     * Application ACK 交易配置（对应岸端 {@code ship/{mmsi}/ack} 的
     * {@code KAFKA_COMMITTED} 确认）。
     *
     * <p>PUBACK 只表示 Broker 已接收，不推进最终游标；只有收到匹配
     * {@code msg_id}（及 {@code seq}，如有）的 Application ACK 后，才推进连续
     * ACK watermark。ACK 可能丢失或重复（QoS1），绝不凭空产生。
     */
    @Data
    public static class AckConfig {
        /** 总开关；关闭时退回 PUBACK 即推进游标的旧语义（兼容未升级岸端）。 */
        private boolean enabled = true;
        /** 发送后多久没收到 ACK 算超时，进入补发集合。 */
        private long ackTimeoutMs = 30000L;
        /** 每个（船舶，表）最多允许的未确认在途数：满了就停发新消息等 ACK。 */
        private int maxInFlight = 200;
    }

    @Data
    public static class PollConfig {
        private int fixedDelayMs = 15000;
        private int initialDelayMs = 5000;
        private int batchSize = 100;
    }

    @Data
    public static class MqttConfig {
        private boolean enabled = true;
        private String brokerUrl = "tcp://broker.emqx.io:1883";
        private String clientId = "smartship_edge_demo";
        private int qos = 1;
        private boolean autoReconnect = true;
        private int keepAliveInterval = 60;
        private int connectionTimeout = 10;
        private String username;
        private String password;
    }
}
