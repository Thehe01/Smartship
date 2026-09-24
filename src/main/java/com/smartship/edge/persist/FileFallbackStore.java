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
 *   <li>磁盘有界：单文件超限即滚动，总量超限删最老文件（调用方对删掉的行计数告警）；</li>
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
                DEFAULT_MAX_FILE_BYTES,
                Math.max(1L, properties.getCollect().getPersist().getFallbackMaxMb()) * 1024L * 1024L);
    }

    /** 测试/单测构造：直接指定目录与上限。 */
    public FileFallbackStore(Path dir, long maxFileBytes, long maxTotalBytes) {
        this.dir = dir;
        this.maxFileBytes = Math.max(1024L, maxFileBytes);
        this.maxTotalBytes = Math.max(4096L, maxTotalBytes);
    }

    /** 待回放文件（最老优先），供回放器遍历。 */
    public record PendingFile(Path path) {
    }

    /** 一条可回放记录。 */
    public record ReplayRecord(String stream, String mmsi, List<TypedArg> args, String rawLine) {
    }

    /** 带类型标签的 JDBC 参数。 */
    public record TypedArg(String t, String v) {
    }

    /**
     * 转存一行可回放记录。返回因总量超限而删掉的旧行数（调用方计数告警）。
     */
    public synchronized long spool(String stream, String mmsi, Object[] args) throws IOException {
        Files.createDirectories(dir);
        rotateIfNeeded();
        byte[] line = (argsToJson(stream, mmsi, args) + "\n").getBytes(StandardCharsets.UTF_8);
        Files.write(activeFile(), line,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.SYNC);
        return enforceTotalCap();
    }

    /** 单行记录编码（文件 spool 与 DB 兜底表共用同一格式，保证两处都可回放）。 */
    public static String argsToJson(String stream, String mmsi, Object[] args) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("v", 1);
        root.put("ts", LocalDateTime.now().toString());
        root.put("stream", stream);
        root.put("mmsi", mmsi == null ? "" : mmsi);
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
                List.copyOf(args));
    }

    public record ParsedLine(String stream, String mmsi, List<TypedArg> args) {
    }

    /** 最老优先列出待回放文件（含当前 active 文件）。 */
    public synchronized List<PendingFile> pendingFiles() throws IOException {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.jsonl")) {
            for (Path p : ds) {
                if (Files.isRegularFile(p) && Files.size(p) > 0) {
                    files.add(p);
                }
            }
        }
        files.sort(Comparator.comparing((Path p) -> p.getFileName().toString().startsWith("failed-writes-") ? 0 : 1)
                .thenComparing(p -> p.getFileName().toString()));
        return files.stream().map(PendingFile::new).toList();
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
                        parsed.stream(), parsed.mmsi(), parsed.args(), line));
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
            Path rotated = dir.resolve("failed-writes-"
                    + java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS")
                            .format(LocalDateTime.now()) + ".jsonl");
            Files.move(active, rotated, StandardCopyOption.ATOMIC_MOVE);
            log.info("[Fallback] spool 滚动: {} ({} bytes)", rotated.getFileName(), Files.size(rotated));
        }
    }

    /** 总量超限删最老文件，返回删掉的行数估计。 */
    private long enforceTotalCap() throws IOException {
        List<PendingFile> files = pendingFiles();
        long total = 0;
        for (PendingFile f : files) {
            total += Files.size(f.path());
        }
        long dropped = 0;
        for (PendingFile f : files) {
            if (total <= maxTotalBytes) {
                break;
            }
            if (f.path().getFileName().toString().equals(ACTIVE_FILE)) {
                continue;
            }
            // 先取值再删：delete 后再 Files.size 会抛 NoSuchFileException。
            long size = Files.size(f.path());
            long lines = countLines(f.path());
            Files.deleteIfExists(f.path());
            total -= size;
            dropped += lines;
            log.warn("[Fallback] 磁盘超限删除最老 spool 文件: {} (约 {} 行)",
                    f.path().getFileName(), lines);
        }
        return dropped;
    }

    private static long countLines(Path p) {
        try (var stream = Files.lines(p, StandardCharsets.UTF_8)) {
            return stream.count();
        } catch (IOException e) {
            return 0;
        }
    }
}
