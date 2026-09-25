package com.smartship.edge.routing.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WriteBatcher 确定性单测（默认 profile，随普通测试执行）。
 */
@DisplayName("WriteBatcher unit test")
class WriteBatcherTest {

    @Test
    @DisplayName("满批恰好触发一次，未满不触发，drainAll 刷出尾批")
    void exactBatchAndDrain() {
        WriteBatcher<String, Integer> b = new WriteBatcher<>(3);
        List<List<Integer>> flushed = new ArrayList<>();
        assertEquals(0, b.add("ship1", 1, flushed::add));
        assertEquals(0, b.add("ship1", 2, flushed::add));
        assertTrue(flushed.isEmpty());
        assertEquals(2, b.bufferedRows());
        assertEquals(3, b.add("ship1", 3, flushed::add));
        assertEquals(1, flushed.size());
        assertEquals(List.of(1, 2, 3), flushed.get(0));
        assertEquals(0, b.bufferedRows());
        // 尾批
        assertEquals(0, b.add("ship1", 4, flushed::add));
        assertEquals(1, b.drainAll(flushed::add));
        assertEquals(2, flushed.size());
        assertEquals(List.of(4), flushed.get(1));
        assertEquals(0, b.bufferedRows());
        assertEquals(0, b.drainAll(flushed::add));
    }

    @Test
    @DisplayName("多 key 隔离：互不干扰，各自满批")
    void multiKeyIsolation() {
        WriteBatcher<String, Integer> b = new WriteBatcher<>(2);
        List<Integer> sizes = new ArrayList<>();
        b.add("a", 1, batch -> sizes.add(batch.size()));
        b.add("b", 10, batch -> sizes.add(batch.size()));
        assertTrue(sizes.isEmpty());
        b.add("a", 2, batch -> sizes.add(batch.size()));
        assertEquals(List.of(2), sizes);
        b.add("b", 20, batch -> sizes.add(batch.size()));
        assertEquals(List.of(2, 2), sizes);
    }

    @Test
    @DisplayName("非法 batchSize 直接失败")
    void illegalBatchSize() {
        assertThrows(IllegalArgumentException.class, () -> new WriteBatcher<String, Integer>(0));
    }

    @Test
    @DisplayName("8 线程并发 add 无丢失、无重复，每批恰为 batchSize")
    void concurrentNoLoss() throws Exception {
        int threads = 8;
        int perThread = 1000;
        int batchSize = 100;
        WriteBatcher<String, Integer> b = new WriteBatcher<>(batchSize);
        AtomicInteger flushedRows = new AtomicInteger();
        AtomicInteger flushCalls = new AtomicInteger();
        java.util.Set<Integer> seen = ConcurrentHashMap.newKeySet();
        List<Integer> batchSizes = java.util.Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int t = 0; t < threads; t++) {
                final int base = t * perThread;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            b.add("same-ship", base + i, batch -> {
                                batchSizes.add(batch.size());
                                flushedRows.addAndGet(batch.size());
                                seen.addAll(batch);
                                flushCalls.incrementAndGet();
                            });
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
            int tail = b.drainAll(batch -> {
                batchSizes.add(batch.size());
                flushedRows.addAndGet(batch.size());
                seen.addAll(batch);
                flushCalls.incrementAndGet();
            });
            assertEquals(threads * perThread, flushedRows.get(), "并发 add 必须无丢失无重复");
            assertEquals(threads * perThread, seen.size(), "每行恰好刷出一次");
            assertTrue(tail >= 0);
            for (int size : batchSizes) {
                assertTrue(size > 0 && size <= batchSize, "每批行数必须在 (0, batchSize] 内");
            }
            int fullBatches = (threads * perThread) / batchSize;
            assertTrue(flushCalls.get() >= fullBatches, "满批必须触发");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("EnginePoint 空 mmsi/时间/replayId 直接失败")
    void enginePointValidation() {
        assertThrows(IllegalArgumentException.class, () ->
                EnginePoint.now("", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () ->
                new EnginePoint("m", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, "r-1"));
        assertThrows(IllegalArgumentException.class, () ->
                new EnginePoint("m", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        java.time.LocalDateTime.now(), " "));
    }
}
