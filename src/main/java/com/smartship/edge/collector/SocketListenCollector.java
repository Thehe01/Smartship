package com.smartship.edge.collector;

import com.smartship.edge.collector.model.BaseInfoVO;
import com.smartship.edge.collector.model.ConfigAlarm;
import com.smartship.edge.collector.model.ConfigDevice;
import com.smartship.edge.collector.model.ConfigVariable;
import com.smartship.edge.collector.model.ICollectService;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Socket 监听采集器
 * <p>
 * 核心特性：
 * 1. 采用容量为 100 的有界 BlockingQueue 解耦底层网络 I/O 读流与上层报文处理
 * 2. 建立天然背压（Backpressure）机制，防止弱网恢复或突发大流量冲垮堆内存触发 OOM
 */
public class SocketListenCollector extends AbstractSocketCollector {

    private int connectIntervalTime = 3000;
    private static final int BUFFER_SIZE = 8192;
    // 有界阻塞队列：容量 100 严格限制内存
    private final BlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<>(100);

    @Override
    protected String getCollectorName() {
        return "Socket-Listen";
    }

    @Override
    protected int getSoTimeout() {
        return this.connectIntervalTime;
    }

    @Override
    public void init(ICollectService collectService, ConfigDevice device, List<ConfigVariable> variables, List<ConfigAlarm> alarms) {
        if (device != null && device.getBaseInfo() != null && device.getBaseInfo().getConnectIntervalTime() != null) {
            this.connectIntervalTime = device.getBaseInfo().getConnectIntervalTime();
        }
        super.init(collectService, device, variables, alarms);
        startTimeoutWatchdog(this.connectIntervalTime * 3L);
    }

    @Override
    protected void initExecutor() {
        BaseInfoVO baseInfo = this.device != null ? this.device.getBaseInfo() : null;
        int queryLength = (baseInfo != null && baseInfo.getQueryLength() != null) ? baseInfo.getQueryLength() : 0;
        int frameLength = (baseInfo != null && baseInfo.getFrameLength() != null) ? baseInfo.getFrameLength() : 0;
        long intervalTime = (baseInfo != null && baseInfo.getIntervalTime() != null) ? baseInfo.getIntervalTime() : 1000L;

        // 1. 读流线程（Producer）：负责快速抓取网络字节帧
        this.executor.submit(() -> {
            while (this.running && !Thread.currentThread().isInterrupted()) {
                try {
                    if (!this.connect || this.socket == null || this.socket.isClosed()) {
                        Thread.sleep(intervalTime);
                        continue;
                    }
                    InputStream inputStream = this.socket.getInputStream();
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead = inputStream.read(buffer);
                    if (bytesRead == -1) {
                        log.warn("[Socket-Listen] 设备: {} 连接被对端断开", getDeviceName());
                        this.connect = false;
                        closeSocketQuietly();
                        continue;
                    }

                    if (bytesRead > 0) {
                        this.lastDataTime = System.currentTimeMillis();
                        int startIndex = (bytesRead >= queryLength + frameLength && queryLength > 0) ? queryLength : 0;
                        int copyLen = frameLength > 0 ? Math.min(frameLength, bytesRead - startIndex) : bytesRead;
                        byte[] frame = new byte[copyLen];
                        System.arraycopy(buffer, startIndex, frame, 0, copyLen);

                        // 阻塞推入有界队列，满载时自然阻塞当前读线程形成背压
                        this.dataQueue.put(frame);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    this.connect = false;
                    closeSocketQuietly();
                    log.error("[Socket-Listen] 设备: {} 读取异常: {}", getDeviceName(), e.getMessage());
                }
            }
        });

        // 2. 报文消费解析线程（Consumer）：负责 CPU 密集型解析计算
        this.executor.submit(() -> {
            while (this.running && !Thread.currentThread().isInterrupted()) {
                try {
                    byte[] chunk = this.dataQueue.take();
                    if (chunk.length > 0) {
                        processFrame(chunk);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("[Socket-Listen] 设备: {} 解析异常: {}", getDeviceName(), e.getMessage());
                }
            }
        });
    }

    protected void processFrame(byte[] frame) {
        log.debug("[Socket-Listen] 设备: {} 处理有效帧长: {}", getDeviceName(), frame.length);
        // 实际场景交由具体驱动服务根据变量配置映射寄存器
    }
}
