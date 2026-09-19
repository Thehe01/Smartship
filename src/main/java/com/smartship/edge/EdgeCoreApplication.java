package com.smartship.edge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 智慧船舶边缘端核心引擎启动类
 *
 * @author SmartShip Architecture Team
 */
@EnableAsync
@EnableScheduling
@SpringBootApplication
public class EdgeCoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(EdgeCoreApplication.class, args);
    }
}
