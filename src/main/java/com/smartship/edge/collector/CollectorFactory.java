package com.smartship.edge.collector;

import com.smartship.edge.collect.modbus.ModbusParser;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 采集器生产装配工厂 (P1-3.2 Collector Production Wiring Closure)
 * <p>
 * 核心架构职责（仅依赖装配与实例创建，不做业务解析）：
 * 1. 生产创建 {@link SocketPollingCollector} 的唯一入口：构造时即注入 Spring 管理的
 *    {@link ModbusParser}，杜绝无 Parser 的采集器进入生产运行导致业务数据静默丢失
 * 2. 设备级实例隔离：每次调用返回全新 Collector 实例（per-device stateful），
 *    但所有实例共享同一个无状态的 {@link ModbusParser} Spring Bean
 * 3. 生命周期仍由 Collector 自身负责（init / connect / collect / close），Factory
 *    不创建线程、不解析 Modbus、不写数据库、不管理 MQTT / Hikari
 */
@Component
@RequiredArgsConstructor
public class CollectorFactory {

    private final ModbusParser modbusParser;

    /**
     * 创建主动轮询采集器（Modbus TCP），自动装配共享 ModbusParser
     *
     * @return 已注入 ModbusParser 的全新 per-device 采集器实例
     */
    public SocketPollingCollector createPollingCollector() {
        return new SocketPollingCollector(modbusParser);
    }

    /**
     * 创建被动监听采集器
     *
     * @return 全新 per-device 采集器实例
     */
    public SocketListenCollector createListenCollector() {
        return new SocketListenCollector();
    }
}
