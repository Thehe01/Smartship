package com.smartship.edge.persist;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 磁盘 spool：追加 SYNC、有序回读、滚动与总量上限。
 */
class FileFallbackStoreTest {

    @TempDir
    Path tempDir;

    private FileFallbackStore store() {
        return new FileFallbackStore(tempDir, 1024L, 10L * 1024L);
    }

    @Test
    @DisplayName("spool 三行按序读回，参数类型精确还原")
    void spoolAndReadInOrder() throws Exception {
        FileFallbackStore store = store();
        Object[] args = new Object[]{"S001", "413999999", 12.5, 1001L, true, null,
                LocalDateTime.of(2026, 9, 19, 10, 0, 0)};
        store.spool("gps", "413999999", args);
        store.spool("wind", "413999999", new Object[]{"x"});

        List<FileFallbackStore.PendingFile> files = store.pendingFiles();
        assertEquals(1, files.size());
        FileFallbackStore.ReadResult read = store.readAll(files.get(0));
        assertEquals(2, read.records().size());
        assertEquals(0, read.badLines());

        FileFallbackStore.ReplayRecord first = read.records().get(0);
        assertEquals("gps", first.stream());
        assertEquals("413999999", first.mmsi());
        List<Object> decoded = first.args().stream().map(FileFallbackStore::decode).toList();
        assertEquals("S001", decoded.get(0));
        assertEquals(12.5, decoded.get(2));
        assertEquals(1001L, decoded.get(3));
        assertEquals(Boolean.TRUE, decoded.get(4));
        assertEquals(null, decoded.get(5));
        assertEquals(LocalDateTime.of(2026, 9, 19, 10, 0, 0), decoded.get(6));
    }

    @Test
    @DisplayName("rewrite 只保留剩余行，空则删文件")
    void rewriteKeepsRemaining() throws Exception {
        FileFallbackStore store = store();
        store.spool("gps", "m", new Object[]{"a"});
        store.spool("gps", "m", new Object[]{"b"});
        FileFallbackStore.PendingFile file = store.pendingFiles().get(0);
        FileFallbackStore.ReadResult read = store.readAll(file);

        store.rewrite(file, List.of(read.records().get(1).rawLine()));
        FileFallbackStore.ReadResult again = store.readAll(file);
        assertEquals(1, again.records().size());

        store.rewrite(file, List.of());
        assertTrue(store.pendingFiles().isEmpty(), "空文件必须删除");
        assertTrue(Files.list(tempDir).findAny().isEmpty());
    }

    @Test
    @DisplayName("单文件上限按总量派生：低配置不撑爆，高配置不频繁滚动")
    void derivedMaxFileBytesBounds() {
        assertEquals(256L * 1024L, FileFallbackStore.derivedMaxFileBytes(1024L * 1024L),
                "1MB 总量 → 256KB 单文件");
        assertEquals(10L * 1024L * 1024L,
                FileFallbackStore.derivedMaxFileBytes(100L * 1024L * 1024L),
                "100MB 总量 → 10MB 封顶");
        assertEquals(64L * 1024L, FileFallbackStore.derivedMaxFileBytes(4096L),
                "极小总量 → 64KB 保底");
    }

    @Test
    @DisplayName("单文件超限滚动，总量超限删最老并返回行数")
    void rotationAndCap() throws Exception {
        // 单文件 1KB 上限：大数据行触发滚动
        FileFallbackStore store = new FileFallbackStore(tempDir, 1024L, 10L * 1024L);
        String big = "x".repeat(800);
        store.spool("gps", "m", new Object[]{big});
        store.spool("gps", "m", new Object[]{big});
        store.spool("gps", "m", new Object[]{big});
        assertEquals(2, store.pendingFiles().size(), "超限必须滚动出第二文件");

        // 总量 4KB 上限：继续写触发删最老（回归：删后取值曾抛 NoSuchFileException）
        FileFallbackStore capped = new FileFallbackStore(tempDir, 1024L, 4096L);
        long droppedTotal = 0;
        for (int i = 0; i < 8; i++) {
            droppedTotal += capped.spool("gps", "m", new Object[]{big});
        }
        long total = 0;
        for (FileFallbackStore.PendingFile f : capped.pendingFiles()) {
            total += Files.size(f.path());
        }
        assertTrue(total <= 4096L + 2048L, "总量必须有界，实际=" + total);
        assertTrue(droppedTotal > 0, "超限必须删最老文件并返回行数，实际=" + droppedTotal);
    }
}
