package com.smartship.edge.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.boot.SpringBootVersion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * P1-4 统一 Benchmark 结果记录器（测试作用域，不污染生产代码）。
 * <p>
 * 职责：
 * 1. 收集各场景原始计数、延迟样本（纳秒）与观测峰值，只做描述性统计
 *    （throughput / p50 / p95 / p99 / max / median），不输出任何预设性能结论；
 * 2. 每个场景完成后立即落盘 {@code target/benchmark/scenario-<name>.json}，
 *    测试失败也不丢失已完成场景；
 * 3. {@link #flush()} 将全部已完成场景汇总为
 *    {@code benchmark-summary.json / .csv / benchmark-report.md}。
 */
public class BenchmarkRecorder {

    private static final Path BENCH_DIR = Paths.get("target", "benchmark");

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    /** 跨测试类的已完成场景注册表（surefire 默认同 JVM 顺序执行，写时加锁）。 */
    private static final Map<String, ScenarioResult> COMPLETED = new LinkedHashMap<>();

    private final Map<String, String> runParams;

    public BenchmarkRecorder(Map<String, String> runParams) {
        this.runParams = runParams != null ? new LinkedHashMap<>(runParams) : Map.of();
    }

    public BenchmarkRecorder() {
        this(Map.of());
    }

    // ==================== 场景构建 ====================

    public Scenario scenario(String name) {
        return new Scenario(name);
    }

    public static final class Scenario {
        private final String name;
        private final Map<String, String> params = new LinkedHashMap<>();
        private final Map<String, Long> counters = new LinkedHashMap<>();
        private final Map<String, Double> peaks = new LinkedHashMap<>();
        private final List<Long> latenciesNanos = new ArrayList<>();
        private final List<Double> runThroughputs = new ArrayList<>();
        private final List<String> notes = new ArrayList<>();

        private Scenario(String name) {
            this.name = name;
        }

        public Scenario param(String key, Object value) {
            params.put(key, String.valueOf(value));
            return this;
        }

        public Scenario count(String key, long delta) {
            counters.merge(key, delta, Long::sum);
            return this;
        }

        public Scenario observePeak(String key, double value) {
            peaks.merge(key, value, Math::max);
            return this;
        }

        public Scenario recordLatencyNanos(long nanos) {
            latenciesNanos.add(Math.max(0, nanos));
            return this;
        }

        public Scenario addRunThroughput(double perSec) {
            runThroughputs.add(perSec);
            return this;
        }

        public Scenario note(String note) {
            notes.add(note);
            return this;
        }

        public String name() {
            return name;
        }
    }

    /**
     * 完成一个场景：计算统计、落盘单场景文件并刷新汇总。每次场景结束调用一次；
     * 建议在 {@code @AfterAll} 再调一次 {@link #flush()} 兜底。
     */
    public ScenarioResult complete(Scenario scenario) {
        List<Long> sorted = new ArrayList<>(scenario.latenciesNanos);
        Collections.sort(sorted);
        ScenarioResult result = new ScenarioResult(
                scenario.name,
                Map.copyOf(scenario.params),
                Map.copyOf(scenario.counters),
                Map.copyOf(scenario.peaks),
                sorted.size(),
                sorted.isEmpty() ? 0.0 : toMillis(percentile(sorted, 50)),
                sorted.isEmpty() ? 0.0 : toMillis(percentile(sorted, 95)),
                sorted.isEmpty() ? 0.0 : toMillis(percentile(sorted, 99)),
                sorted.isEmpty() ? 0.0 : toMillis(sorted.get(sorted.size() - 1)),
                medianDouble(scenario.runThroughputs),
                List.copyOf(scenario.runThroughputs),
                List.copyOf(scenario.notes));
        synchronized (COMPLETED) {
            COMPLETED.put(result.name(), result);
        }
        try {
            Files.createDirectories(BENCH_DIR);
            writeJson(BENCH_DIR.resolve("scenario-" + sanitize(result.name()) + ".json"),
                    Map.of("scenario", result.asMap(), "env", envMeta(runParams)));
        } catch (IOException e) {
            System.err.println("[Benchmark] 场景文件落盘失败: " + result.name() + ": " + e.getMessage());
        }
        flush();
        return result;
    }

    // ==================== 汇总输出 ====================

    /**
     * 基于磁盘上全部 {@code scenario-*.json} 重建三份汇总文件。
     * 只汇总真实存在的结果文件，不虚构未运行场景。
     */
    public void flush() {
        try {
            Files.createDirectories(BENCH_DIR);
            Map<String, ScenarioResult> all = loadAll();
            List<Map<String, Object>> rows = new ArrayList<>();
            for (ScenarioResult r : all.values()) {
                rows.add(r.asMap());
            }
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("env", envMeta(runParams));
            summary.put("scenarios", rows);
            writeJson(BENCH_DIR.resolve("benchmark-summary.json"), summary);
            Files.writeString(BENCH_DIR.resolve("benchmark-summary.csv"), toCsv(all), StandardCharsets.UTF_8);
            Files.writeString(BENCH_DIR.resolve("benchmark-report.md"), toMarkdown(all), StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[Benchmark] 汇总落盘失败: " + e.getMessage());
        }
    }

    private static Map<String, ScenarioResult> loadAll() {
        Map<String, ScenarioResult> merged = new LinkedHashMap<>();
        synchronized (COMPLETED) {
            merged.putAll(COMPLETED);
        }
        if (!Files.isDirectory(BENCH_DIR)) {
            return merged;
        }
        try (var stream = Files.list(BENCH_DIR)) {
            for (Path p : (Iterable<Path>) stream.filter(f -> {
                String n = f.getFileName().toString();
                return n.startsWith("scenario-") && n.endsWith(".json");
            })::iterator) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> root = MAPPER.readValue(p.toFile(), Map.class);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> s = (Map<String, Object>) root.get("scenario");
                    if (s != null && s.get("name") != null) {
                        merged.putIfAbsent(String.valueOf(s.get("name")), ScenarioResult.fromMap(s));
                    }
                } catch (Exception ignored) {
                    // 单个损坏文件不影响其他场景
                }
            }
        } catch (IOException ignored) {
        }
        // 按场景名排序，保证输出稳定可 diff
        Map<String, ScenarioResult> sorted = new TreeMap<>(merged);
        return sorted;
    }

    // ==================== 环境元数据 ====================

    public static Map<String, Object> envMeta(Map<String, String> params) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("timestamp", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
                Instant.now().atOffset(ZoneOffset.ofHours(8))));
        env.put("java_version", System.getProperty("java.version", "unknown"));
        env.put("available_processors", Runtime.getRuntime().availableProcessors());
        env.put("max_memory_mb", Runtime.getRuntime().maxMemory() / 1024 / 1024);
        env.put("os", System.getProperty("os.name", "unknown") + " "
                + System.getProperty("os.version", ""));
        env.put("spring_boot_version", SpringBootVersion.getVersion());
        env.put("git_commit", gitCommit());
        env.put("params", params != null ? new LinkedHashMap<>(params) : Map.of());
        return env;
    }

    private static String gitCommit() {
        try {
            Process proc = new ProcessBuilder("git", "rev-parse", "--short", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            boolean done = proc.waitFor(5, TimeUnit.SECONDS);
            if (!done) {
                proc.destroyForcibly();
                return "unknown";
            }
            String out = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            return out.isEmpty() ? "unknown" : out;
        } catch (Exception e) {
            return "unknown";
        }
    }

    // ==================== 统计工具 ====================

    public static long percentile(List<Long> sortedAscending, double p) {
        if (sortedAscending.isEmpty()) {
            return 0L;
        }
        int idx = (int) Math.ceil(p / 100.0 * sortedAscending.size()) - 1;
        return sortedAscending.get(Math.min(Math.max(idx, 0), sortedAscending.size() - 1));
    }

    public static double medianDouble(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    }

    private static double toMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    // ==================== 文件格式 ====================

    private static void writeJson(Path path, Object value) throws IOException {
        Files.writeString(path, MAPPER.writeValueAsString(value), StandardCharsets.UTF_8);
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private static String toCsv(Map<String, ScenarioResult> all) {
        StringBuilder sb = new StringBuilder(
                "scenario,latency_samples,throughput_median_per_sec,p50_ms,p95_ms,p99_ms,max_ms\n");
        for (ScenarioResult r : all.values()) {
            sb.append(csv(r.name())).append(',')
                    .append(r.latencySamples()).append(',')
                    .append(fmt(r.throughputMedian())).append(',')
                    .append(fmt(r.p50Ms())).append(',')
                    .append(fmt(r.p95Ms())).append(',')
                    .append(fmt(r.p99Ms())).append(',')
                    .append(fmt(r.maxMs())).append('\n');
        }
        return sb.toString();
    }

    private static String csv(String s) {
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }

    private static String fmt(double v) {
        return String.format(java.util.Locale.ROOT, "%.3f", v);
    }

    private static String toMarkdown(Map<String, ScenarioResult> all) {
        StringBuilder sb = new StringBuilder("# SmartShip Edge Benchmark Report\n\n");
        sb.append("> 本报告全部数字来自本次真实运行的原始测量；未运行场景不会出现在本文件中。\n");
        sb.append("> 测量值仅描述当前测试环境（见 Environment），不得直接宣称为生产环境性能。\n\n");
        sb.append("## Environment\n\n");
        Map<String, Object> env = envMeta(Map.of());
        for (Map.Entry<String, Object> e : env.entrySet()) {
            if ("params".equals(e.getKey())) {
                continue;
            }
            sb.append("- ").append(e.getKey()).append(": ").append(e.getValue()).append('\n');
        }
        for (ScenarioResult r : all.values()) {
            sb.append("\n## Scenario – ").append(r.name()).append('\n');
            if (!r.params().isEmpty()) {
                sb.append("\nParameters:\n");
                for (Map.Entry<String, String> e : r.params().entrySet()) {
                    sb.append("- ").append(e.getKey()).append(": ").append(e.getValue()).append('\n');
                }
            }
            if (!r.counters().isEmpty()) {
                sb.append("\nCounters:\n");
                for (Map.Entry<String, Long> e : r.counters().entrySet()) {
                    sb.append("- ").append(e.getKey()).append(": ").append(e.getValue()).append('\n');
                }
            }
            if (!r.peaks().isEmpty()) {
                sb.append("\nPeaks:\n");
                for (Map.Entry<String, Double> e : r.peaks().entrySet()) {
                    sb.append("- ").append(e.getKey()).append(": ").append(fmt(e.getValue())).append('\n');
                }
            }
            sb.append("\nLatency (ms over ").append(r.latencySamples()).append(" samples):\n");
            sb.append("- p50: ").append(fmt(r.p50Ms())).append('\n');
            sb.append("- p95: ").append(fmt(r.p95Ms())).append('\n');
            sb.append("- p99: ").append(fmt(r.p99Ms())).append('\n');
            sb.append("- max: ").append(fmt(r.maxMs())).append('\n');
            sb.append("\nThroughput:\n");
            sb.append("- median_per_sec: ").append(fmt(r.throughputMedian())).append('\n');
            if (!r.runThroughputs().isEmpty()) {
                sb.append("- runs_per_sec: [");
                for (int i = 0; i < r.runThroughputs().size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(fmt(r.runThroughputs().get(i)));
                }
                sb.append("]\n");
            }
            if (!r.notes().isEmpty()) {
                sb.append("\nNotes:\n");
                for (String n : r.notes()) {
                    sb.append("- ").append(n).append('\n');
                }
            }
        }
        return sb.toString();
    }

    // ==================== 不可变结果 ====================

    public record ScenarioResult(
            String name,
            Map<String, String> params,
            Map<String, Long> counters,
            Map<String, Double> peaks,
            int latencySamples,
            double p50Ms,
            double p95Ms,
            double p99Ms,
            double maxMs,
            double throughputMedian,
            List<Double> runThroughputs,
            List<String> notes) {

        Map<String, Object> asMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", name);
            m.put("params", params);
            m.put("counters", counters);
            m.put("peaks", peaks);
            m.put("latency_samples", latencySamples);
            m.put("p50_ms", p50Ms);
            m.put("p95_ms", p95Ms);
            m.put("p99_ms", p99Ms);
            m.put("max_ms", maxMs);
            m.put("throughput_median_per_sec", throughputMedian);
            m.put("throughput_runs_per_sec", runThroughputs);
            m.put("notes", notes);
            return m;
        }

        @SuppressWarnings("unchecked")
        static ScenarioResult fromMap(Map<String, Object> m) {
            return new ScenarioResult(
                    String.valueOf(m.get("name")),
                    (Map<String, String>) m.getOrDefault("params", Map.of()),
                    numMap(m.get("counters")),
                    dblMap(m.get("peaks")),
                    ((Number) m.getOrDefault("latency_samples", 0)).intValue(),
                    ((Number) m.getOrDefault("p50_ms", 0)).doubleValue(),
                    ((Number) m.getOrDefault("p95_ms", 0)).doubleValue(),
                    ((Number) m.getOrDefault("p99_ms", 0)).doubleValue(),
                    ((Number) m.getOrDefault("max_ms", 0)).doubleValue(),
                    ((Number) m.getOrDefault("throughput_median_per_sec", 0)).doubleValue(),
                    dblList(m.get("throughput_runs_per_sec")),
                    strList(m.get("notes")));
        }

        private static Map<String, Long> numMap(Object o) {
            Map<String, Long> out = new LinkedHashMap<>();
            if (o instanceof Map<?, ?> m) {
                m.forEach((k, v) -> out.put(String.valueOf(k), ((Number) v).longValue()));
            }
            return out;
        }

        private static Map<String, Double> dblMap(Object o) {
            Map<String, Double> out = new LinkedHashMap<>();
            if (o instanceof Map<?, ?> m) {
                m.forEach((k, v) -> out.put(String.valueOf(k), ((Number) v).doubleValue()));
            }
            return out;
        }

        private static List<Double> dblList(Object o) {
            List<Double> out = new ArrayList<>();
            if (o instanceof List<?> l) {
                l.forEach(v -> out.add(((Number) v).doubleValue()));
            }
            return out;
        }

        private static List<String> strList(Object o) {
            List<String> out = new ArrayList<>();
            if (o instanceof List<?> l) {
                l.forEach(v -> out.add(String.valueOf(v)));
            }
            return out;
        }
    }
}
