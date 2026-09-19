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
 * 周期性向下位机 PLC / 动力系统发送 Modbus TCP MBAP 读寄存器指令，
 * 基于 MBAP Header Length 契约流式分包，规避 TCP 半包与粘包，
 * 异步通过有界队列推入业务解算线程。
 */
public class SocketPollingCollector extends AbstractSocketCollector {

    private final AtomicInteger transactionCounter = new AtomicInteger(1);
    private byte[] queryCommand;
    private final BlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<>(100);

    @Override
    protected String getCollectorName() {
        return "Socket-Polling";
    }

    @Override
    public void init(ICollectService collectService, ConfigDevice device, List<ConfigVariable> variables, List<ConfigAlarm> alarms) {
        // 构造默认读保持寄存器指令：MBAP Header (7 字节) + PDU (5 字节) = 12 字节无 CRC 的标准 Modbus TCP 报文
        int txId = transactionCounter.getAndIncrement() & 0xFFFF;
        ModbusTcpRequest request = ModbusTcpRequest.readHoldingRegisters(txId, 1, 0, 16);
        this.queryCommand = ModbusTcpCodec.encodeRequest(request);

        super.init(collectService, device, variables, alarms);
        startTimeoutWatchdog(10000L);
    }

    @Override
    protected void initExecutor() {
        // 1. 发送查询并基于 MBAP 头精准截帧流式读取
        this.executor.submit(() -> {
            while (this.running && !Thread.currentThread().isInterrupted()) {
                try {
                    if (!this.connect || this.socket == null || this.socket.isClosed()) {
                        Thread.sleep(1000L);
                        continue;
                    }
                    OutputStream outputStream = this.socket.getOutputStream();
                    outputStream.write(this.queryCommand);
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

                    this.lastDataTime = System.currentTimeMillis();
                    this.dataQueue.put(frame);

                    Thread.sleep(1000L); // 默认 1 秒轮询一次
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    this.connect = false;
                    closeSocketQuietly();
                    log.error("[Socket-Polling] 设备: {} 轮询异常: {}", getDeviceName(), e.getMessage());
                }
            }
        });

        // 2. 数据处理线程：解码 MBAP 响应与寄存器还原
        this.executor.submit(() -> {
            while (this.running && !Thread.currentThread().isInterrupted()) {
                try {
                    byte[] frame = this.dataQueue.take();
                    ModbusTcpResponse response = ModbusTcpCodec.decodeResponse(frame);
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
}
