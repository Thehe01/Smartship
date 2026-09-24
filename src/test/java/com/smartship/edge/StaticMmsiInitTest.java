package com.smartship.edge;

import com.smartship.edge.collect.nmea.parser.NmeaParser;
import com.smartship.edge.collect.nmea.service.NmeaDataHandler;
import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.ShipLocalInitializer;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import com.smartship.edge.uploader.mqtt.MqttClientManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class StaticMmsiInitTest {

    @Mock
    private NmeaDataHandler dataHandler;

    @Mock
    private NmeaDataPersistenceService persistenceService;

    @Mock
    private ShipLocalInitializer shipAutoRegisterService;

    @Mock
    private MqttClientManager mqttClientManager;

    @Test
    @DisplayName("测试启动时配置静态 MMSI 能够立即触发船舶自注册并就绪分船库结构")
    void testStaticMmsiRegistrationOnInit() {
        EdgeProperties properties = new EdgeProperties();
        properties.setMmsi("413999999");

        NmeaParser parser = new NmeaParser(
                dataHandler,
                persistenceService,
                properties,
                shipAutoRegisterService,
                mqttClientManager
        );

        // 模拟 Spring 容器启动后调用 @PostConstruct init()
        parser.init();

        // 验证必须调用了 shipAutoRegisterService.ensureRegistered("413999999")
        verify(shipAutoRegisterService).ensureRegistered("413999999");
    }
}
