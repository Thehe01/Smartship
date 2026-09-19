package com.smartship.edge.collector;

import com.smartship.edge.collector.model.BaseInfoVO;
import com.smartship.edge.collector.model.ConfigAlarm;
import com.smartship.edge.collector.model.ConfigDevice;
import com.smartship.edge.collector.model.ConfigVariable;
import com.smartship.edge.collector.model.ICollectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 抽象 Socket 采集器基类
 * <p>
 * 核心架构职责：
 * 1. 统一 Socket 建立、超时检测、平滑断开与运行状态同步
 * 2. 治理采集器线程池：统一采用带设备业务上下文的命名守护线程工厂（Daemon ThreadFactory）
 * 3. 规避线程泄漏：在 {@link #close()} 中强制中断并安全终止线程池（shutdownNow + awaitTermination）
 * 4. 内置看门狗（Watchdog）：周期性检测长时间无数据的心跳差，主动自愈关闭 TCP 假死连接
 * 5. 抽取通用的 Modbus 缓冲区切分与寄存器大端序寻址工具
 *
 * @author SmartShip Architecture Team
 */
public abstract class AbstractSocketCollector implements Collectable {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected ICollectService collectService;
    protected ConfigDevice device;
    protected List<ConfigVariable> variables;
    protected List<ConfigAlarm> alarms;

    protected Socket socket;
    protected volatile boolean connect = false;
    protected volatile boolean running = true;
    protected long lastDataTime = System.currentTimeMillis();

    protected ExecutorService executor;
    protected ScheduledExecutorService scheduler;

    @Override
    public void init(ICollectService collectService, ConfigDevice device, List<ConfigVariable> variables, List<ConfigAlarm> alarms) {
        this.collectService = collectService;
        this.device = device;
        this.variables = variables;
        this.alarms = alarms;
        this.running = true;
        this.executor = createExecutor();
        initExecutor();
    }

    /**
     * 子类实现各自的读流、解析工作线程编排
     */
    protected abstract void initExecutor();

    /**
     * 获取采集器名称（用于日志和线程标识）
     */
    protected abstract String getCollectorName();

    /**
     * 获取 Socket 读取超时时间（毫秒），子类可按需重写
     */
    protected int getSoTimeout() {
        return 3000;
    }

    /**
     * 启动超时心跳检测看门狗
     */
    protected void startTimeoutWatchdog(long timeoutMs) {
        if (timeoutMs <= 0) {
            timeoutMs = 10000L;
        }
        long interval = timeoutMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Watchdog-" + getDeviceName());
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(() -> {
            if (this.connect && (System.currentTimeMillis() - this.lastDataTime > interval)) {
                log.error("[Watchdog] 设备: {}, {} 长时间未接收到数据 (超过 {}ms)，触发断开自愈重连",
                        getDeviceName(), getCollectorName(), interval);
                this.connect = false;
                closeSocketQuietly();
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    /**
     * 创建带有明确设备业务语义的守护线程池
     */
    protected ExecutorService createExecutor() {
        String devName = (this.device != null && this.device.getDeviceName() != null) ? this.device.getDeviceName() : "Unknown";
        Long devCode = (this.device != null && this.device.getDeviceCode() != null) ? this.device.getDeviceCode() : 0L;
        AtomicInteger counter = new AtomicInteger(1);
        return Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "Collector-" + devCode + "-" + devName + "-" + counter.getAndIncrement());
            t.setDaemon(true); // 守护线程，不阻塞 JVM 正常退出
            return t;
        });
    }

    @Override
    public synchronized void connect() {
        if (this.connect && this.socket != null && !this.socket.isClosed() && this.socket.isConnected()) {
            return;
        }
        try {
            if (this.device != null && this.device.getBaseInfo() != null) {
                BaseInfoVO baseInfo = this.device.getBaseInfo();
                if (this.collectService != null) {
                    this.collectService.updateDeviceStatus(this.device, "2");
                }
                this.socket = new Socket(baseInfo.getIp(), baseInfo.getPort());
                this.socket.setSoTimeout(getSoTimeout());
                this.connect = true;
                this.lastDataTime = System.currentTimeMillis();
                if (this.collectService != null) {
                    this.collectService.updateDeviceStatus(this.device, "1");
                }
                log.info("[Socket] 设备:{}, {} 已成功连接至 {}:{}",
                        getDeviceName(), getCollectorName(), baseInfo.getIp(), baseInfo.getPort());
            }
        } catch (Exception e) {
            this.connect = false;
            if (this.collectService != null && this.device != null) {
                this.collectService.updateDeviceStatus(this.device, "2");
            }
            closeSocketQuietly();
            log.error("[Socket] 设备:{}, {} 连接异常或超时: {}", getDeviceName(), getCollectorName(), e.getMessage());
        }
    }

    @Override
    public void collect() throws IOException {
        // 默认空实现，由 initExecutor 中的工作线程异步接收推流或轮询
    }

    @Override
    public synchronized void close() {
        this.running = false;
        this.connect = false;
        closeSocketQuietly();
        if (this.scheduler != null && !this.scheduler.isShutdown()) {
            this.scheduler.shutdownNow();
        }
        if (this.executor != null && !this.executor.isShutdown()) {
            this.executor.shutdownNow();
            try {
                if (!this.executor.awaitTermination(2, TimeUnit.SECONDS)) {
                    log.warn("[Socket] 设备:{}, {} 采集器工作线程在等待2秒后强制终止", getDeviceName(), getCollectorName());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 规范：恢复中断状态
            }
        }
    }

    /**
     * 静默安全关闭底层 Socket
     */
    protected void closeSocketQuietly() {
        if (this.socket != null) {
            try {
                if (!this.socket.isClosed()) {
                    this.socket.close();
                }
            } catch (IOException e) {
                log.warn("[Socket] 设备:{}, {} 关闭Socket异常: {}", getDeviceName(), getCollectorName(), e.getMessage());
            } finally {
                this.socket = null;
            }
        }
    }

    /**
     * 截取缓冲区有效字节
     */
    protected byte[] processBuffer(byte[] bytes, int length) {
        byte[] data = new byte[length];
        System.arraycopy(bytes, 0, data, 0, length);
        return data;
    }

    /**
     * 解析 Modbus 报文起始寄存器地址（大端序）
     */
    protected int getStartIndex(byte[] cmd, int offset) {
        if (cmd == null || cmd.length < 4 + offset) {
            return 0;
        }
        int high = cmd[2 + offset] & 0xFF;
        int low = cmd[3 + offset] & 0xFF;
        return (high << 8) | low;
    }

    protected String getDeviceName() {
        return (this.device != null && this.device.getDeviceName() != null) ? this.device.getDeviceName() : "Unknown";
    }
}
