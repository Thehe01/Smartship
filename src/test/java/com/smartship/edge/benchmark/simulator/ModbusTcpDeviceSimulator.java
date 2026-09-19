package com.smartship.edge.benchmark.simulator;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Modbus TCP 设备模拟器（测试作用域，ServerSocket 实现，不引入 Netty）。
 * <p>
 * 行为：读取客户端 MBAP 请求，原样返回相同 {@code transactionId / unitId}，
 * 构造合法 0x03 保持寄存器响应；按配置确定性注入延迟、断开、异常响应、
 * 错误 txId 与畸形帧。全部线程为守护线程，{@link #close()} 后端口释放、无残留。
 */
public final class ModbusTcpDeviceSimulator implements AutoCloseable {

    private final ModbusSimulationConfig config;
    private final Random random;
    private final ServerSocket serverSocket;
    private final ExecutorService workers;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread acceptThread;
    /** 全部已 accept 且尚未退出的 client 连接，close 时逐个关闭以解阻塞 read。 */
    private final Set<Socket> activeSockets = ConcurrentHashMap.newKeySet();

    private final AtomicLong requestsReceived = new AtomicLong();
    private final AtomicLong responsesSent = new AtomicLong();
    private final AtomicLong exceptionsSent = new AtomicLong();
    private final AtomicLong disconnects = new AtomicLong();
    private final AtomicLong malformedSent = new AtomicLong();
    private final AtomicInteger connCounter = new AtomicInteger();

    public ModbusTcpDeviceSimulator(ModbusSimulationConfig config) throws IOException {
        this.config = config;
        this.random = new Random(config.seed());
        this.serverSocket = new ServerSocket(0, 50, java.net.InetAddress.getByName("127.0.0.1"));
        this.workers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "modbus-sim-" + connCounter.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        this.acceptThread = new Thread(this::acceptLoop, "modbus-sim-accept");
        this.acceptThread.setDaemon(true);
        this.acceptThread.start();
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public long requestsReceived() {
        return requestsReceived.get();
    }

    public long responsesSent() {
        return responsesSent.get();
    }

    public long exceptionsSent() {
        return exceptionsSent.get();
    }

    public long disconnects() {
        return disconnects.get();
    }

    public long malformedSent() {
        return malformedSent.get();
    }

    private void acceptLoop() {
        while (running.get()) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                if (!running.get()) {
                    closeQuietly(socket);
                    break;
                }
                activeSockets.add(socket);
                final Socket accepted = socket;
                try {
                    workers.submit(() -> serve(accepted));
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    // close() 并发跑完导致任务被拒：绝不泄漏，必须摘除并关闭该连接
                    activeSockets.remove(accepted);
                    closeQuietly(accepted);
                }
            } catch (IOException e) {
                if (socket != null) {
                    activeSockets.remove(socket);
                    closeQuietly(socket);
                }
                if (running.get()) {
                    // 运行期 accept 异常仅记录，关闭期属于正常退出
                    System.err.println("[ModbusSim] accept 异常: " + e.getMessage());
                }
                break;
            }
        }
    }

    private void serve(Socket socket) {
        try (socket) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            while (running.get() && !socket.isClosed()) {
                byte[] header = new byte[7];
                try {
                    readFully(in, header, 0, 7);
                } catch (EOFException e) {
                    break; // 对端关闭
                }
                int length = ((header[4] & 0xFF) << 8) | (header[5] & 0xFF);
                if (length < 2 || length > 260) {
                    break;
                }
                byte[] rest = new byte[length - 1];
                try {
                    readFully(in, rest, 0, rest.length);
                } catch (EOFException e) {
                    break;
                }
                // 请求至少 unitId(1) + fc(1) + addr(2) + qty(2) = 6 字节后续
                if (rest.length < 5) {
                    break;
                }
                int txId = ((header[0] & 0xFF) << 8) | (header[1] & 0xFF);
                int unitId = header[6] & 0xFF;
                int function = rest[0] & 0xFF;
                int quantity = ((rest[3] & 0xFF) << 8) | (rest[4] & 0xFF);
                quantity = Math.min(Math.max(quantity, 0), 125);
                requestsReceived.incrementAndGet();

                double r = nextRandom();
                if (r < config.disconnectProbability()) {
                    disconnects.incrementAndGet();
                    break; // 直接断开，不响应
                }
                if (config.responseDelayMs() > 0) {
                    try {
                        Thread.sleep(config.responseDelayMs());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                r = nextRandom();
                if (r < config.exceptionResponseProbability()) {
                    out.write(buildException(txId, unitId, function, 0x02));
                    out.flush();
                    exceptionsSent.incrementAndGet();
                    continue;
                }
                r = nextRandom();
                if (r < config.wrongTransactionIdProbability()) {
                    out.write(buildReadResponse((txId + 1) & 0xFFFF, unitId, function, quantity));
                    out.flush();
                    responsesSent.incrementAndGet();
                    continue;
                }
                r = nextRandom();
                if (r < config.malformedFrameProbability()) {
                    // 声称 50 字节但只发 3 字节后断开，客户端 readFully 必抛异常
                    out.write(new byte[]{header[0], header[1], 0x00, 0x00, 0x00, 0x32, (byte) unitId});
                    out.flush();
                    malformedSent.incrementAndGet();
                    break;
                }
                out.write(buildReadResponse(txId, unitId, function, quantity));
                out.flush();
                responsesSent.incrementAndGet();
            }
        } catch (IOException ignored) {
            // 客户端断开属于正常现象
        } finally {
            activeSockets.remove(socket);
        }
    }

    private synchronized double nextRandom() {
        return random.nextDouble();
    }

    private static byte[] buildReadResponse(int txId, int unitId, int function, int quantity) {
        int byteCount = quantity * 2;
        int length = 1 + 1 + 1 + byteCount;
        byte[] frame = new byte[6 + length];
        frame[0] = (byte) ((txId >> 8) & 0xFF);
        frame[1] = (byte) (txId & 0xFF);
        frame[2] = 0x00;
        frame[3] = 0x00;
        frame[4] = (byte) ((length >> 8) & 0xFF);
        frame[5] = (byte) (length & 0xFF);
        frame[6] = (byte) (unitId & 0xFF);
        frame[7] = (byte) (function & 0xFF);
        frame[8] = (byte) byteCount;
        for (int i = 0; i < quantity; i++) {
            int val = (i + 1) * 100; // 确定性寄存器值：rpm=10.0, temp=20.0, ...
            frame[9 + i * 2] = (byte) ((val >> 8) & 0xFF);
            frame[10 + i * 2] = (byte) (val & 0xFF);
        }
        return frame;
    }

    private static byte[] buildException(int txId, int unitId, int function, int code) {
        byte[] frame = new byte[9];
        frame[0] = (byte) ((txId >> 8) & 0xFF);
        frame[1] = (byte) (txId & 0xFF);
        frame[2] = 0x00;
        frame[3] = 0x00;
        frame[4] = 0x00;
        frame[5] = 0x03;
        frame[6] = (byte) (unitId & 0xFF);
        frame[7] = (byte) ((function | 0x80) & 0xFF);
        frame[8] = (byte) (code & 0xFF);
        return frame;
    }

    private static void closeQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static void readFully(InputStream in, byte[] buf, int off, int len) throws IOException {        int total = 0;
        while (total < len) {
            int n = in.read(buf, off + total, len - total);
            if (n == -1) {
                throw new EOFException("对端关闭");
            }
            total += n;
        }
    }

    /** 仅供测试使用的内省接口：当前存活 client 连接数。 */
    public int activeConnectionCount() {
        return activeSockets.size();
    }

    /** 仅供测试使用的内省接口：worker 池是否已终止。 */
    public boolean isTerminated() {
        return workers.isTerminated();
    }

    /** 仅供测试使用的内省接口：accept 线程是否仍存活。 */
    public boolean isAcceptThreadAlive() {
        return acceptThread.isAlive();
    }

    /**
     * 幂等关闭：停 accept → 关 client sockets（解阻塞 read）→ 关 serverSocket →
     * shutdown workers 并等待 → join accept 线程。连续调用不得异常。
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        running.set(false);
        for (Socket s : activeSockets) {
            try {
                s.close();
            } catch (IOException ignored) {
            }
        }
        try {
            serverSocket.close();
        } catch (IOException ignored) {
        }
        workers.shutdownNow();
        try {
            workers.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            acceptThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
