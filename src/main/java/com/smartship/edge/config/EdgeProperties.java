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
    private Datasource datasource = new Datasource();
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
    public static class Datasource {
        private ShipShipDataSourceProperties ship = new ShipShipDataSourceProperties();
    }

    @Data
    public static class ShipShipDataSourceProperties {
        private String defaultHost = "localhost";
        private int defaultPort = 3306;
        private String defaultUsername = "root";
        private String defaultPassword = "123456";
        private int maximumPoolSize = 5;
        private String jdbcParams = "useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true&useSSL=false";
        private String driverClassName = "";
        private long registryValidationIntervalMs = 5000L;
    }

    @Data
    public static class Uploader {
        private boolean enabled = true;
        private PollConfig poll = new PollConfig();
        private MqttConfig mqtt = new MqttConfig();
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
