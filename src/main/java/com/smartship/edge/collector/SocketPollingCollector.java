package com.smartship.edge.collector;

import com.smartship.edge.collector.model.ConfigAlarm;
import com.smartship.edge.collector.model.ConfigDevice;
import com.smartship.edge.collector.model.ConfigVariable;
import com.smartship.edge.collector.model.ICollectService;
import com.smartship.edge.collector.utils.ModbusUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Socket 主动轮询采集器
 * <p>
 * 周期性向下位机 PLC 发送 Modbus 读寄存器指令，并异步接收与处理响应
 */
public class SocketPollingCollector extends AbstractSocketCollector {

    private byte[] queryCommand;
    private final BlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<>(100);

    @Override
    protected String getCollectorName() {
        return "Socket-Polling";
    }

    @Override
    public void init(ICollectService collectService, ConfigDevice device, List<ConfigVariable> variables, List<ConfigAlarm> alarms) {
        // 构造默认读保持寄存器指令：从站 1，从 0 号寄存器开始，读取 16 个寄存器，功能码 0x03
        this.queryCommand = ModbusUtils.buildReadRequest(1, 0, 16, 0x03);
        super.init(collectService, device, variables, alarms);
        startTimeoutWatchdog(10000L);
    }

    @Override
    protected void initExecutor() {
        // 1. 发送查询并读取响应线程
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
                    byte[] buffer = new byte[1024];
                    int len = inputStream.read(buffer);
                    if (len > 0) {
                        this.lastDataTime = System.currentTimeMillis();
                        byte[] frame = new byte[len];
                        System.arraycopy(buffer, 0, frame, 0, len);
                        this.dataQueue.put(frame);
                    }
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

        // 2. 数据处理线程
        this.executor.submit(() -> {
            while (this.running && !Thread.currentThread().isInterrupted()) {
                try {
                    byte[] frame = this.dataQueue.take();
                    List<Integer> registers = ModbusUtils.parseResponse(frame);
                    if (!registers.isEmpty()) {
                        log.debug("[Socket-Polling] 设备: {} 解析得到 {} 个寄存器数值", getDeviceName(), registers.size());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("[Socket-Polling] 设备: {} 解析异常: {}", getDeviceName(), e.getMessage());
                }
            }
        });
    }
}
