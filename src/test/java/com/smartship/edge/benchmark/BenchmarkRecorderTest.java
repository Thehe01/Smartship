package com.smartship.edge.benchmark;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P1-4.1 Benchmark 完整性单元测试（正确性测试，随默认 {@code mvn test} 执行，非 benchmark）。
 * <p>
 * 覆盖：run 目录隔离、不读旧 run、median/percentile 数学、A/B ratio 口径、
 * 报告 run_id 与无外来场景。
 */
@DisplayName("P1-4.1 Integrity: BenchmarkRecorder")
class BenchmarkRecorderTest {

    @Test
    @DisplayName("同一 JVM 的 Recorder 共享 runDir，跨实例可见")
    void recordersShareRunDir() {
        BenchmarkRecorder a = new BenchmarkRecorder(Map.of("k", "v"));
        BenchmarkRecorder b = new BenchmarkRecorder(Map.of());
        BenchmarkRecorder.Scenario s = a.scenario("integrity-share-probe");
        s.count("n", 1);
        a.complete(s);
        assertTrue(BenchmarkRunContext.runDir().resolve("scenario-integrity-share-probe.json")
                .toFile().exists());
        // 另一个 Recorder 的汇总必须包含该场景（同 run 共享）
        b.flush();
        String summary = readSummary();
        assertTrue(summary.contains("integrity-share-probe"));
    }

    @Test
    @DisplayName("根目录旧 run 文件不被读入本轮报告")
    void foreignRunFilesIgnored() throws Exception {
        Path foreign = Paths.get("target", "benchmark", "scenario-foreign-probe.json");
        Files.createDirectories(foreign.getParent());
        Files.writeString(foreign, "{\"scenario\":{\"name\":\"foreign-probe\"}}", StandardCharsets.UTF_8);
        try {
            new BenchmarkRecorder().flush();
            String summary = readSummary();
            assertFalse(summary.contains("foreign-probe"), "旧 run 文件不得混入本轮报告");
        } finally {
            Files.deleteIfExists(foreign);
        }
    }

    @Test
    @DisplayName("median 与 percentile 数学正确")
    void mathCorrect() {
        assertEquals(2.0, BenchmarkRecorder.medianDouble(List.of(1.0, 2.0, 3.0)));
        assertEquals(2.5, BenchmarkRecorder.medianDouble(List.of(1.0, 2.0, 3.0, 4.0)));
        assertEquals(0.0, BenchmarkRecorder.medianDouble(List.of()));
        assertEquals(30L, BenchmarkRecorder.percentile(List.of(10L, 20L, 30L), 95));
        assertEquals(10L, BenchmarkRecorder.percentile(List.of(10L, 20L, 30L), 1));
        assertEquals(0L, BenchmarkRecorder.percentile(List.of(), 95));
        assertEquals(2.0, BenchmarkRecorder.percentileDouble(List.of(1.0, 2.0, 3.0), 50));
    }

    @Test
    @DisplayName("A/B throttle ratio 口径：同 dataset 行数比，基线为 0 时 N/A")
    void throttleRatioSemantics() {
        assertEquals(0.9, BenchmarkRecorder.throttleReductionRatio(100, 10), 1e-9);
        assertEquals(0.0, BenchmarkRecorder.throttleReductionRatio(100, 100), 1e-9);
        assertNull(BenchmarkRecorder.throttleReductionRatio(0, 0), "baseline_rows==0 必须 N/A");
        assertNull(BenchmarkRecorder.throttleReductionRatio(0, 5), "baseline_rows==0 必须 N/A");
    }

    @Test
    @DisplayName("报告含 run_id 且仅含本轮场景")
    void reportHasRunId() {
        BenchmarkRecorder recorder = new BenchmarkRecorder();
        BenchmarkRecorder.Scenario s = recorder.scenario("integrity-runid-probe");
        s.measure("op", 1.5);
        recorder.complete(s);
        String summary = readSummary();
        assertTrue(summary.contains(BenchmarkRunContext.runId()), "汇总必须包含本轮 run_id");
        assertFalse(summary.contains("foreign-probe"));
        Path report = BenchmarkRunContext.runDir().resolve("benchmark-report.md");
        assertTrue(report.toFile().exists());
        String md = read(report);
        assertTrue(md.contains("Run ID: " + BenchmarkRunContext.runId()));
        assertTrue(md.contains("integrity-runid-probe"));
    }

    private static String readSummary() {
        return read(BenchmarkRunContext.runDir().resolve("benchmark-summary.json"));
    }

    private static String read(Path p) {
        try {
            return Files.readString(p, StandardCharsets.UTF_8);
        } catch (Exception e) {
            fail("报告文件应存在: " + p + ": " + e.getMessage());
            throw new IllegalStateException(e);
        }
    }
}
