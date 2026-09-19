package com.smartship.edge;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PersistenceThrottle;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PersistenceThrottleTest {

    @Test
    @DisplayName("测试无锁 CAS 写入节流单线程拦截与防抖")
    void testThrottleSingleThread() throws InterruptedException {
        EdgeProperties props = new EdgeProperties();
        props.getCollect().getPersist().setMinWriteIntervalSeconds(1); // 1 秒最小间隔

        PersistenceThrottle throttle = new PersistenceThrottle(props);
        String key = "413999999:nmea:gps";

        // 第 1 次应当放行
        assertTrue(throttle.shouldWrite(key), "首次写入应当成功放行");

        // 紧接着多次高频调用，应当全部被拦截
        for (int i = 0; i < 10; i++) {
            assertFalse(throttle.shouldWrite(key), "1秒窗口内的高频重复写入应当被拦截");
        }

        // 等待超过 1 秒后
        Thread.sleep(1100);
        assertTrue(throttle.shouldWrite(key), "超过 1 秒后应当再次允许写入");
    }

    @Test
    @DisplayName("测试多线程高并发下 CAS 节流的原子性与削峰效果")
    void testThrottleConcurrent() throws InterruptedException {
        EdgeProperties props = new EdgeProperties();
        props.getCollect().getPersist().setMinWriteIntervalSeconds(2);

        PersistenceThrottle throttle = new PersistenceThrottle(props);
        String key = "413999999:modbus:engine:1";

        int threads = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    if (throttle.shouldWrite(key)) {
                        successCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        // 在同一时刻突发 50 个线程竞争同一 key，只允许严格放行 1 次！
        assertEquals(1, successCount.get(), "并发瞬时调用下，CAS 应保证有且仅有 1 个线程抢到写入资格");
    }
}
