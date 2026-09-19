package com.smartship.edge.benchmark;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark 运行上下文（P1-4.1 run isolation）。
 * <p>
 * 同一 JVM 内只初始化一次：所有 {@link BenchmarkRecorder} 共享同一个
 * {@code RUN_ID / RUN_DIR}，保证一次 {@code mvn test -Pbenchmark} 的全部场景
 * 落盘到同一轮目录，报告绝不混入历史 run。
 */
public final class BenchmarkRunContext {

    private static final String RUN_ID = createRunId();
    private static final Path RUN_DIR = Paths.get("target", "benchmark", RUN_ID);
    private static final String STARTED_AT = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
            Instant.now().atOffset(ZoneOffset.ofHours(8)));

    private BenchmarkRunContext() {
    }

    /** 例如 {@code 20260919-231500-9cce540}，git 不可用时为 {@code ...-unknown}。 */
    public static String runId() {
        return RUN_ID;
    }

    /** 例如 {@code target/benchmark/20260919-231500-9cce540}。 */
    public static Path runDir() {
        return RUN_DIR;
    }

    public static String startedAt() {
        return STARTED_AT;
    }

    private static String createRunId() {
        String ts = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(
                Instant.now().atOffset(ZoneOffset.ofHours(8)));
        return ts + "-" + gitCommit();
    }

    static String gitCommit() {
        try {
            Process proc = new ProcessBuilder("git", "rev-parse", "--short", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            boolean done = proc.waitFor(5, TimeUnit.SECONDS);
            if (!done) {
                proc.destroyForcibly();
                return "unknown";
            }
            String out = new String(proc.getInputStream().readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8).trim();
            return out.isEmpty() ? "unknown" : out;
        } catch (Exception e) {
            return "unknown";
        }
    }
}
