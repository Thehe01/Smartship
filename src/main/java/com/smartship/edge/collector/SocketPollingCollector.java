package com.smartship.edge.collector;

import com.smartship.edge.collect.modbus.tcp.ModbusTcpCodec;
import com.smartship.edge.collect.modbus.tcp.ModbusTcpRequest;
import com.smartship.edge.collect.modbus.tcp.ModbusTcpResponse;
import com.smartship.edge.collector.model.ConfigAlarm;
import com.smartship.edge.collector.model.ConfigDevice;
import com.smartship.edge.collector.model.ConfigVariable;
import com.smartship.edge.collector.model.ICollectService;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Socket 主动轮询采集器 (Modbus TCP)
 * <p>
 * 核心架构规范：
 * 1. 每轮请求动态自增 Transaction ID，实现严格的请求-响应事务级匹配校验（Transaction ID + Unit ID + Function Code）
 * 2. 基于 MBAP Header Length 契约流式分包，利用 readFully 消除 TCP 半包与粘包
 * 3. 异步通过有界阻塞队列（容量 100）解耦网络 I/O 与业务解算线程，形成天然背压
 */
public class SocketPollingCollector extends AbstractSocketCollector {

    private final AtomicInteger transactionCounter = new AtomicInteger(1);
    private int unitId = 1;
    private int startAddress = 0;
    private int quantity = 16;
    private long pollIntervalMs = 1000L;

    private final BlockingQueue<ModbusTcpResponse> responseQueue = new LinkedBlockingQueue<>(100);

    @Override
    protected String getCollectorName() {
        return "Socket-Polling";
    }

    @Override
    public void init(ICollectService collectService, ConfigDevice device, List<ConfigVariable> variables, List<ConfigAlarm> alarms) {
        super.init(collectService, device, variables, alarms);
        startTimeoutWatchdog(10000L);
    }

    @Override
    protected void initExecutor() {
        // 1. 发送查询并基于 MBAP 头精准截帧流式读取与事务一致性校验
        this.executor.submit(() -> {
            while (this.running && !Thread.currentThread().isInterrupted()) {
                try {
                    if (!this.connect || this.socket == null || this.socket.isClosed()) {
                        Thread.sleep(pollIntervalMs);
                        continue;
                    }

                    // 每轮请求动态自增生成 Transaction ID，确保请求与响应严格关联闭环
                    int txId = transactionCounter.getAndIncrement() & 0xFFFF;
                    ModbusTcpRequest request = ModbusTcpRequest.readHoldingRegisters(txId, unitId, startAddress, quantity);

                    OutputStream outputStream = this.socket.getOutputStream();
                    outputStream.write(ModbusTcpCodec.encodeRequest(request));
                    outputStream.flush();

                    InputStream inputStream = this.socket.getInputStream();
                    // 基于 MBAP Header 中声明的 Length 字段，利用 readFully 消除 TCP 粘包半包
                    byte[] frame = ModbusTcpCodec.readFrame(inputStream);
                    if (frame == null) {
                        log.warn("[Socket-Polling] 设备: {} 连接被对端断开", getDeviceName());
                        this.connect = false;
                        closeSocketQuietly();
                        continue;
                    }

                    ModbusTcpResponse response = ModbusTcpCodec.decodeResponse(frame);

                    // 严格校验响应报文与发起请求的事务一致性（Transaction ID, Unit ID, Function Code）
                    ModbusTcpCodec.validateResponse(request, response);

                    this.lastDataTime = System.currentTimeMillis();
                    this.responseQueue.put(response);

                    Thread.sleep(pollIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    this.connect = false;
                    closeSocketQuietly();
                    log.error("[Socket-Polling] 设备: {} 轮询/校验异常: {}", getDeviceName(), e.getMessage());
                }
            }
        });

        // 2. 数据处理线程：解耦消费已通过 MBAP 事务校验的合法响应
        this.executor.submit(() -> {
            while (this.running && !Thread.currentThread().isInterrupted()) {
                try {
                    ModbusTcpResponse response = this.responseQueue.take();
                    if (response.isException()) {
                        log.warn("[Socket-Polling] 设备: {} 收到 Modbus 异常响应: 功能码 0x{}, 异常码 0x{}",
                                getDeviceName(), Integer.toHexString(response.functionCode()), Integer.toHexString(response.exceptionCode()));
                    } else if (response.registers().length > 0) {
                        log.debug("[Socket-Polling] 设备: {} 从站: {} 成功解出 {} 个寄存器数值",
                                getDeviceName(), response.unitId(), response.registers().length);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("[Socket-Polling] 设备: {} 解码处理异常: {}", getDeviceName(), e.getMessage());
                }
            }
        });
    }

    public int getTransactionCount() {
        return transactionCounter.get();
    }
}
