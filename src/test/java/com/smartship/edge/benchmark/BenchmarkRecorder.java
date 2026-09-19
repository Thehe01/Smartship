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

/**
 * P1-4 统一 Benchmark 结果记录器（P1-4.1 run isolation + named measurements）。
 * <p>
 * 规则：
 * 1. 所有写入只进当前 {@link BenchmarkRunContext#runDir()}，{@code loadAll} 只扫描
 *    本轮目录，报告绝不混入历史 run；
 * 2. 不同语义的延迟必须用命名组区分（{@link Scenario#recordLatency}），禁止混算一个 percentile；
 *    单值型测量（recovery_ms / refresh_ms）用 {@link Scenario#measure}，报告给出 runs + median；
 * 3. 只做描述性统计，不输出任何预设性能结论；削减率必须经
 *    {@link #throttleReductionRatio} 由实测行数计算。
 */
public class BenchmarkRecorder {

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
        /** 命名测量组 → 毫秒样本（recordLatency 换算自纳秒，measure 直接记毫秒）。 */
        private final Map<String, List<Double>> measuresMs = new LinkedHashMap<>();
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

        /** 命名延迟样本（纳秒），不同语义必须用不同组名。 */
        public Scenario recordLatency(String group, long nanos) {
            measuresMs.computeIfAbsent(group, k -> new ArrayList<>()).add(Math.max(0, nanos) / 1_000_000.0);
            return this;
        }

        /** 兼容旧调用：归入 {@code default} 组；新代码应使用命名组。 */
        public Scenario recordLatencyNanos(long nanos) {
            return recordLatency("default", nanos);
        }

        /** 单值型测量（毫秒），如 recovery_ms / refresh_ms：每轮 measured 记录一次。 */
        public Scenario measure(String group, double millis) {
            measuresMs.computeIfAbsent(group, k -> new ArrayList<>()).add(millis);
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
     * 完成一个场景：计算统计、落盘本轮单场景文件并刷新本轮汇总。
     * 每次场景结束调用一次；建议在 {@code @AfterAll} 再调一次 {@link #flush()} 兜底。
     */
    public ScenarioResult complete(Scenario scenario) {
        Map<String, MeasureStats> groups = new LinkedHashMap<>();
        for (Map.Entry<String, List<Double>> e : scenario.measuresMs.entrySet()) {
            groups.put(e.getKey(), MeasureStats.of(e.getValue()));
        }
        ScenarioResult result = new ScenarioResult(
                scenario.name,
                Map.copyOf(scenario.params),
                Map.copyOf(scenario.counters),
                Map.copyOf(scenario.peaks),
                groups,
                medianDouble(scenario.runThroughputs),
                List.copyOf(scenario.runThroughputs),
                List.copyOf(scenario.notes));
        synchronized (COMPLETED) {
            COMPLETED.put(result.name(), result);
        }
        try {
            Path dir = BenchmarkRunContext.runDir();
            Files.createDirectories(dir);
            writeJson(dir.resolve("scenario-" + sanitize(result.name()) + ".json"),
                    Map.of("scenario", result.asMap(), "env", envMeta(runParams)));
        } catch (IOException e) {
            System.err.println("[Benchmark] 场景文件落盘失败: " + result.name() + ": " + e.getMessage());
        }
        flush();
        return result;
    }

    // ==================== 汇总输出（仅本轮） ====================

    /**
     * 基于本轮目录 + 内存注册表重建三份汇总文件，并刷新 {@code latest-run.txt}。
     * 只汇总本轮真实存在的结果，不扫描历史 run。
     */
    public void flush() {
        try {
            Path dir = BenchmarkRunContext.runDir();
            Files.createDirectories(dir);
            Map<String, ScenarioResult> all = loadAll();
            List<Map<String, Object>> rows = new ArrayList<>();
            for (ScenarioResult r : all.values()) {
                rows.add(r.asMap());
            }
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("env", envMeta(runParams));
            summary.put("scenarios", rows);
            writeJson(dir.resolve("benchmark-summary.json"), summary);
            Files.writeString(dir.resolve("benchmark-summary.csv"), toCsv(all), StandardCharsets.UTF_8);
            Files.writeString(dir.resolve("benchmark-report.md"), toMarkdown(all), StandardCharsets.UTF_8);
            Files.writeString(Paths.get("target", "benchmark", "latest-run.txt"),
                    BenchmarkRunContext.runId() + "\n", StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[Benchmark] 汇总落盘失败: " + e.getMessage());
        }
    }

    /** 只读本轮目录（+ 内存），绝不递归历史 run。 */
    static Map<String, ScenarioResult> loadAll() {
        Map<String, ScenarioResult> merged = new LinkedHashMap<>();
        synchronized (COMPLETED) {
            merged.putAll(COMPLETED);
        }
        Path dir = BenchmarkRunContext.runDir();
        if (Files.isDirectory(dir)) {
            try (var stream = Files.list(dir)) {
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
        }
        return new TreeMap<>(merged);
    }

    // ==================== 环境元数据 ====================

    public static Map<String, Object> envMeta(Map<String, String> params) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("run_id", BenchmarkRunContext.runId());
        env.put("run_started_at", BenchmarkRunContext.startedAt());
        env.put("timestamp", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
                Instant.now().atOffset(ZoneOffset.ofHours(8))));
        env.put("java_version", System.getProperty("java.version", "unknown"));
        env.put("available_processors", Runtime.getRuntime().availableProcessors());
        env.put("max_memory_mb", Runtime.getRuntime().maxMemory() / 1024 / 1024);
        env.put("os", System.getProperty("os.name", "unknown") + " "
                + System.getProperty("os.version", ""));
        env.put("spring_boot_version", SpringBootVersion.getVersion());
        env.put("git_commit", BenchmarkRunContext.gitCommit());
        env.put("params", params != null ? new LinkedHashMap<>(params) : Map.of());
        return env;
    }

    // ==================== 统计工具 ====================

    public static long percentile(List<Long> sortedAscending, double p) {
        if (sortedAscending.isEmpty()) {
            return 0L;
        }
        int idx = (int) Math.ceil(p / 100.0 * sortedAscending.size()) - 1;
        return sortedAscending.get(Math.min(Math.max(idx, 0), sortedAscending.size() - 1));
    }

    public static double percentileDouble(List<Double> values, double p) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.min(Math.max(idx, 0), sorted.size() - 1));
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

    /**
     * PersistenceThrottle 写入削减率（严格 A/B 口径）。
     *
     * @param baselineRows  同一 dataset 在 throttle=0 下的实测入库行数
     * @param throttledRows 同一 dataset 在 throttle>0 下的实测入库行数
     * @return {@code 1 - throttled/baseline}；baseline 为 0 时返回 {@code null}（N/A），禁止除以有效消息数
     */
    public static Double throttleReductionRatio(long baselineRows, long throttledRows) {
        if (baselineRows <= 0) {
            return null;
        }
        return 1.0 - (double) throttledRows / baselineRows;
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
                "scenario,measure_group,samples,throughput_median_per_sec,p50_ms,p95_ms,p99_ms,max_ms,median_ms\n");
        for (ScenarioResult r : all.values()) {
            if (r.groups().isEmpty()) {
                sb.append(csv(r.name())).append(",,0,")
                        .append(fmt(r.throughputMedian())).append(",,,,,\n");
            } else {
                for (Map.Entry<String, MeasureStats> g : r.groups().entrySet()) {
                    MeasureStats st = g.getValue();
                    sb.append(csv(r.name())).append(',').append(csv(g.getKey())).append(',')
                            .append(st.count()).append(',')
                            .append(fmt(r.throughputMedian())).append(',')
                            .append(fmt(st.p50())).append(',')
                            .append(fmt(st.p95())).append(',')
                            .append(fmt(st.p99())).append(',')
                            .append(fmt(st.max())).append(',')
                            .append(fmt(st.median())).append('\n');
                }
            }
        }
        return sb.toString();
    }

    private static String csv(String s) {
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }

    static String fmt(double v) {
        return String.format(java.util.Locale.ROOT, "%.3f", v);
    }

    private static String toMarkdown(Map<String, ScenarioResult> all) {
        StringBuilder sb = new StringBuilder("# SmartShip Edge Benchmark Report\n\n");
        sb.append("> 本报告全部数字来自本轮独立实验的原始测量；未运行场景不会出现。\n");
        sb.append("> 测量值仅描述当前测试环境，不得直接宣称为生产环境性能。\n\n");
        Map<String, Object> env = envMeta(Map.of());
        sb.append("Run ID: ").append(env.get("run_id")).append('\n');
        sb.append("Git Commit: ").append(env.get("git_commit")).append('\n');
        sb.append("Started At: ").append(env.get("run_started_at")).append('\n');
        sb.append("\n## Environment\n\n");
        for (Map.Entry<String, Object> e : env.entrySet()) {
            if ("params".equals(e.getKey()) || "run_id".equals(e.getKey())
                    || "run_started_at".equals(e.getKey())) {
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
            for (Map.Entry<String, MeasureStats> g : r.groups().entrySet()) {
                MeasureStats st = g.getValue();
                sb.append("\nLatency[").append(g.getKey()).append("] (ms over ")
                        .append(st.count()).append(" samples):\n");
                sb.append("- p50: ").append(fmt(st.p50())).append('\n');
                sb.append("- p95: ").append(fmt(st.p95())).append('\n');
                sb.append("- p99: ").append(fmt(st.p99())).append('\n');
                sb.append("- max: ").append(fmt(st.max())).append('\n');
                sb.append("- median: ").append(fmt(st.median())).append('\n');
                if (!st.values().isEmpty() && st.count() <= 32) {
                    sb.append("- runs: [");
                    for (int i = 0; i < st.values().size(); i++) {
                        if (i > 0) {
                            sb.append(", ");
                        }
                        sb.append(fmt(st.values().get(i)));
                    }
                    sb.append("]\n");
                }
            }
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

    public record MeasureStats(int count, double p50, double p95, double p99,
                               double max, double median, List<Double> values) {
        static MeasureStats of(List<Double> millis) {
            List<Double> sorted = new ArrayList<>(millis);
            Collections.sort(sorted);
            return new MeasureStats(
                    sorted.size(),
                    percentileDouble(sorted, 50),
                    percentileDouble(sorted, 95),
                    percentileDouble(sorted, 99),
                    sorted.isEmpty() ? 0.0 : sorted.get(sorted.size() - 1),
                    medianDouble(sorted),
                    List.copyOf(sorted));

        }

        Map<String, Object> asMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("samples", count);
            m.put("p50_ms", p50);
            m.put("p95_ms", p95);
            m.put("p99_ms", p99);
            m.put("max_ms", max);
            m.put("median_ms", median);
            m.put("values_ms", values);
            return m;
        }

        @SuppressWarnings("unchecked")
        static MeasureStats fromMap(Map<String, Object> m) {
            List<Double> values = new ArrayList<>();
            Object v = m.get("values_ms");
            if (v instanceof List<?> l) {
                l.forEach(x -> values.add(((Number) x).doubleValue()));
            }
            return new MeasureStats(
                    ((Number) m.getOrDefault("samples", values.size())).intValue(),
                    ((Number) m.getOrDefault("p50_ms", 0)).doubleValue(),
                    ((Number) m.getOrDefault("p95_ms", 0)).doubleValue(),
                    ((Number) m.getOrDefault("p99_ms", 0)).doubleValue(),
                    ((Number) m.getOrDefault("max_ms", 0)).doubleValue(),
                    ((Number) m.getOrDefault("median_ms", 0)).doubleValue(),
                    values);
        }
    }

    public record ScenarioResult(
            String name,
            Map<String, String> params,
            Map<String, Long> counters,
            Map<String, Double> peaks,
            Map<String, MeasureStats> groups,
            double throughputMedian,
            List<Double> runThroughputs,
            List<String> notes) {

        Map<String, Object> asMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", name);
            m.put("params", params);
            m.put("counters", counters);
            m.put("peaks", peaks);
            Map<String, Object> g = new LinkedHashMap<>();
            for (Map.Entry<String, MeasureStats> e : groups.entrySet()) {
                g.put(e.getKey(), e.getValue().asMap());
            }
            m.put("measure_groups", g);
            m.put("throughput_median_per_sec", throughputMedian);
            m.put("throughput_runs_per_sec", runThroughputs);
            m.put("notes", notes);
            return m;
        }

        @SuppressWarnings("unchecked")
        static ScenarioResult fromMap(Map<String, Object> m) {
            Map<String, MeasureStats> groups = new LinkedHashMap<>();
            Object g = m.get("measure_groups");
            if (g instanceof Map<?, ?> gm) {
                gm.forEach((k, v) -> groups.put(String.valueOf(k),
                        MeasureStats.fromMap((Map<String, Object>) v)));
            }
            return new ScenarioResult(
                    String.valueOf(m.get("name")),
                    (Map<String, String>) m.getOrDefault("params", Map.of()),
                    numMap(m.get("counters")),
                    dblMap(m.get("peaks")),
                    groups,
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
