package com.smartship.edge.persist;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.smartship.edge.config.EdgeProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 本地兜底 spool：MySQL 整体故障（主表 + 兜底表都写不进）时的磁盘 JSONL 保险。
 *
 * <p>设计要点：
 * <ul>
 *   <li>每行一条可回放记录 {@code {stream, mmsi, args:[{t,v}]}}，参数带类型标签，
 *   回放时精确还原 JDBC 参数（含 LocalDateTime）；</li>
 *   <li>追加写带 {@code SYNC}，掉电最多丢最后一行；</li>
 *   <li>磁盘有界：单文件超限即滚动，总量超限删最老文件（调用方对删掉的行计数告警）；
 *   单文件上限按总量派生（总量/4，夹在 64KB~10MB），低配置下总量依然严格有界
 *   （超限最多一个正在写的 active 文件）；</li>
 *   <li>所有方法 {@code synchronized}：采集线程 spool 与回放线程 drain/rewrite
 *   同一 JVM 内互斥，无需文件锁。</li>
 * </ul>
 */
@Slf4j
@Component
public class FileFallbackStore {

    static final String ACTIVE_FILE = "failed-writes.jsonl";
    private static final long DEFAULT_MAX_FILE_BYTES = 10L * 1024 * 1024;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path dir;
    private final long maxFileBytes;
    private final long maxTotalBytes;

    @Autowired
    public FileFallbackStore(EdgeProperties properties) {
        this(Path.of(properties.getCollect().getPersist().getFallbackDir()),
                derivedMaxFileBytes(
                        Math.max(1L, properties.getCollect().getPersist().getFallbackMaxMb())
                                * 1024L * 1024L),
                Math.max(1L, properties.getCollect().getPersist().getFallbackMaxMb())
                        * 1024L * 1024L);
    }

    /**
     * 单文件上限派生：总量/4，夹在 64KB~10MB。
     * <p>固定 10MB 单文件 + 可配小总量（如 1MB）会导致实际用量远超配置；
     * 派生后超限最多一个 active 文件（active 文件永不参与容量清理）。
     */
    static long derivedMaxFileBytes(long maxTotalBytes) {
        return Math.min(DEFAULT_MAX_FILE_BYTES, Math.max(64L * 1024L, maxTotalBytes / 4));
    }

    /** 测试/单测构造：直接指定目录与上限。 */
    public FileFallbackStore(Path dir, long maxFileBytes, long maxTotalBytes) {
        this.dir = dir;
        this.maxFileBytes = Math.max(1024L, maxFileBytes);
        this.maxTotalBytes = Math.max(1024L, maxTotalBytes);
    }

    /** 待回放文件（最老优先），供回放器遍历。 */
    public record PendingFile(Path path) {
    }

    /** 一条可回放记录（replayId 恒定：崩溃重试复用同一幂等键）。 */
    public record ReplayRecord(
            String stream, String mmsi, String replayId, List<TypedArg> args, String rawLine) {
    }

    /** 带类型标签的 JDBC 参数。 */
    public record TypedArg(String t, String v) {
    }

    /**
     * 转存一行可回放记录。返回本轮丢失行数（超限删除的旧行 + 超大单行拒收计 1，
     * 调用方计数告警）。正常路径返回 0。
     *
     * <p>严格有界：写前先把总量（含 active 文件）+ 本行压到上限内——必要时先滚动
     * active 使其参与清理；单行超过总量上限直接拒收。写后复检兜底。因此实际占用
     * 永不超过 {@code maxTotalBytes + 一行}，而不是“上限 + 整个 active 文件”。
     */
    public synchronized long spool(String stream, String mmsi, Object[] args) throws IOException {
        Files.createDirectories(dir);
        byte[] line = (argsToJson(stream, mmsi, args) + "\n").getBytes(StandardCharsets.UTF_8);
        if (line.length > maxTotalBytes) {
            log.warn("[Fallback] 单行 {} bytes 超总量上限 {}，拒绝写入并计数丢失",
                    line.length, maxTotalBytes);
            return 1;
        }
        rotateIfNeeded();
        // 压力滚动：总量（含本行）已超限时先滚动 active，使其参与清理；
        // 否则只剩 active 可写时实际占用会无界跟随 active 增长。
        if (totalBytes() + line.length > maxTotalBytes) {
            rotateActive();
        }
        long dropped = enforceTotalCap(line.length);
        Files.write(activeFile(), line,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.SYNC);
        dropped += enforceTotalCap(0);
        return dropped;
    }

    /** 单行记录编码（文件 spool 与 DB 兜底表共用同一格式，保证两处都可回放）。 */
    public static String argsToJson(String stream, String mmsi, Object[] args) {
        return argsToJson(stream, mmsi, java.util.UUID.randomUUID().toString(), args);
    }

    /** 同上，replay_id 由调用方指定（测试 pin 住确定性时使用）。 */
    public static String argsToJson(String stream, String mmsi, String replayId, Object[] args) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("v", 1);
        root.put("ts", LocalDateTime.now().toString());
        root.put("stream", stream);
        root.put("mmsi", mmsi == null ? "" : mmsi);
        root.put("replay_id", replayId == null ? "" : replayId);
        ArrayNode arr = root.putArray("args");
        if (args != null) {
            for (Object a : args) {
                arr.add(MAPPER.valueToTree(encode(a)));
            }
        }
        return root.toString();
    }

    /** 解析单行记录；格式不对抛 IllegalArgumentException（调用方跳过保留现场）。 */
    public static ParsedLine parseLine(String line) {
        final JsonNode root;
        try {
            root = MAPPER.readTree(line);
        } catch (Exception e) {
            throw new IllegalArgumentException("not JSON: " + e.getMessage(), e);
        }
        JsonNode arr = root.path("args");
        if (!root.isObject() || !arr.isArray()) {
            throw new IllegalArgumentException("missing args[]");
        }
        List<TypedArg> args = new ArrayList<>();
        for (JsonNode n : arr) {
            args.add(new TypedArg(n.path("t").asText("s"), n.path("v").asText(null)));
        }
        return new ParsedLine(
                root.path("stream").asText(""),
                root.path("mmsi").asText(""),
                normalizeReplayId(root.path("replay_id").asText(null)),
                List.copyOf(args));
    }

    public record ParsedLine(String stream, String mmsi, String replayId, List<TypedArg> args) {
    }

    /** 空 replay_id 视为缺失（老版本 spool 行）：回放用 NULL，不与他人冲突。 */
    private static String normalizeReplayId(String raw) {
        return raw == null || raw.isBlank() ? null : raw;
    }

    /** 最老优先列出待回放文件（含当前 active 文件）。 */
    public synchronized List<PendingFile> pendingFiles() throws IOException {
        return listDataFiles().stream()
                .filter(p -> {
                    try {
                        return Files.size(p) > 0;
                    } catch (IOException e) {
                        return false;
                    }
                })
                .map(PendingFile::new)
                .toList();
    }

    /** 全部 spool 数据文件（rotated 优先、active 最后；隐藏 .tmp 中间文件）。 */
    private List<Path> listDataFiles() throws IOException {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.jsonl")) {
            for (Path p : ds) {
                if (Files.isRegularFile(p)) {
                    files.add(p);
                }
            }
        }
        files.sort(Comparator.comparing((Path p) -> p.getFileName().toString().startsWith("failed-writes-") ? 0 : 1)
                .thenComparing(p -> p.getFileName().toString()));
        return files;
    }

    private long totalBytes() throws IOException {
        long total = 0;
        for (Path p : listDataFiles()) {
            total += Files.size(p);
        }
        return total;
    }

    /** 读出文件全部行并解析；坏行跳过并计数返回（调用方可告警）。 */
    public synchronized ReadResult readAll(PendingFile file) throws IOException {
        List<ReplayRecord> records = new ArrayList<>();
        int badLines = 0;
        for (String line : Files.readAllLines(file.path(), StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            try {
                ParsedLine parsed = parseLine(line);
                records.add(new ReplayRecord(
                        parsed.stream(), parsed.mmsi(), parsed.replayId(), parsed.args(), line));
            } catch (Exception e) {
                badLines++;
                log.warn("[Fallback] spool 坏行跳过: {}", e.getMessage());
            }
        }
        return new ReadResult(records, badLines);
    }

    public record ReadResult(List<ReplayRecord> records, int badLines) {
    }

    /**
     * 回写剩余行（原子 tmp+move）；空则删文件。崩溃窗口只影响“已回放成功但未提交”
     * 的行（下次重放，幂等由业务键/去重保证；本地时序表允许重复行，上传端按 id
     * 增量，重复行只会多传一次，岸端 UNIQUE(msg_id) 吸收——与现有 at-least-once
     * 语义一致）。
     */
    public synchronized void rewrite(PendingFile file, List<String> remainingLines) throws IOException {
        if (remainingLines == null || remainingLines.isEmpty()) {
            Files.deleteIfExists(file.path());
            return;
        }
        Path tmp = file.path().resolveSibling(file.path().getFileName() + ".tmp");
        String body = String.join("\n", remainingLines) + "\n";
        Files.write(tmp, body.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.SYNC);
        Files.move(tmp, file.path(),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /** 把 {@link TypedArg} 还原为 JDBC 参数。 */
    public static Object decode(TypedArg a) {
        if (a == null || a.t() == null) {
            return null;
        }
        return switch (a.t()) {
            case "null" -> null;
            case "i" -> toLong(a.v());
            case "d" -> toDouble(a.v());
            case "b" -> Boolean.parseBoolean(a.v());
            case "ts" -> a.v() == null ? null : LocalDateTime.parse(a.v());
            default -> a.v();
        };
    }

    private static Long toLong(String v) {
        if (v == null) {
            return null;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double toDouble(String v) {
        if (v == null) {
            return null;
        }
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static TypedArg encode(Object a) {
        if (a == null) {
            return new TypedArg("null", null);
        }
        if (a instanceof Boolean b) {
            return new TypedArg("b", b.toString());
        }
        if (a instanceof Integer || a instanceof Long || a instanceof Short || a instanceof Byte) {
            return new TypedArg("i", String.valueOf(a));
        }
        if (a instanceof Double || a instanceof Float || a instanceof java.math.BigDecimal) {
            return new TypedArg("d", String.valueOf(a));
        }
        if (a instanceof LocalDateTime t) {
            return new TypedArg("ts", t.toString());
        }
        if (a instanceof java.sql.Timestamp ts) {
            return new TypedArg("ts", ts.toLocalDateTime().toString());
        }
        if (a instanceof java.util.Date d) {
            return new TypedArg("ts", new java.sql.Timestamp(d.getTime()).toLocalDateTime().toString());
        }
        return new TypedArg("s", String.valueOf(a));
    }

    private Path activeFile() {
        return dir.resolve(ACTIVE_FILE);
    }

    private void rotateIfNeeded() throws IOException {
        Path active = activeFile();
        if (Files.exists(active) && Files.size(active) >= maxFileBytes) {
            rotateActive();
        }
    }

    private void rotateActive() throws IOException {
        Path active = activeFile();
        if (!Files.exists(active) || Files.size(active) == 0) {
            return;
        }
        Path rotated = dir.resolve("failed-writes-"
                + java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS")
                        .format(LocalDateTime.now()) + ".jsonl");
        Files.move(active, rotated, StandardCopyOption.ATOMIC_MOVE);
        log.info("[Fallback] spool 滚动: {} ({} bytes)", rotated.getFileName(), Files.size(rotated));
    }

    /**
     * 总量预检（含 active 文件 + 即将写入的字节）：删最老非 active 文件直到 fits。
     * active 文件永不直接删除——压力过大时调用方应先 {@link #rotateActive}。
     */
    private long enforceTotalCap(long incomingBytes) throws IOException {
        long dropped = 0;
        while (true) {
            List<Path> files = listDataFiles();
            long total = 0;
            for (Path p : files) {
                total += Files.size(p);
            }
            if (total + incomingBytes <= maxTotalBytes) {
                return dropped;
            }
            Path victim = null;
            for (Path p : files) {
                if (!p.getFileName().toString().equals(ACTIVE_FILE)) {
                    victim = p;
                    break;
                }
            }
            if (victim == null) {
                return dropped;
            }
            // 先取值再删：delete 后再 Files.size 会抛 NoSuchFileException。
            long size = Files.size(victim);
            long lines = countLines(victim);
            Files.deleteIfExists(victim);
            dropped += lines;
            log.warn("[Fallback] 磁盘超限删除最老 spool 文件: {} (约 {} 行, {} bytes)",
                    victim.getFileName(), lines, size);
        }
    }

    /** 历史兼容：无预检的复检（写后兜底）。 */
    private long enforceTotalCap() throws IOException {
        return enforceTotalCap(0);
    }

    private static long countLines(Path p) {
        try (var stream = Files.lines(p, StandardCharsets.UTF_8)) {
            return stream.count();
        } catch (IOException e) {
            return 0;
        }
    }
}
