package com.smartship.edge.persist;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.observability.SmartShipMetrics;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 兜底回放器：周期性把兜底数据重写入主表。
 *
 * <p>顺序：先 DB 兜底表（{zh}zncb_failed_writes{en}，按 id 升序），再磁盘 spool
 * （最老文件优先），共享单轮预算，避免长停机后打爆 DB。
 *
 * <p>语义：
 * <ul>
 *   <li>DB 行回放成功即删行；回放失败（DB 仍不可用）即停 DB 阶段，但文件阶段照常
 *   ——文件阶段同样首败即停，避免热循环；</li>
 *   <li>单行解析失败/未知流（毒行，含旧版诊断文本行）跳过保留现场：内存记次，
 *   超 {@code MAX_ROW_ATTEMPTS} 后永久跳过（行保留供人工审计，不再刷日志）；</li>
 *   <li>insert 与 delete 非原子：崩溃窗口内重复回放产生重复行——与现有
 *   at-least-once 语义一致（上传端按 id 增量，岸端 UNIQUE(msg_id) 吸收）。</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FallbackReplayer {

    /** 单行内存重试上限：超限后跳过（行保留，供人工审计）。 */
    static final int MAX_ROW_ATTEMPTS = 5;

    private final JdbcTemplate jdbcTemplate;
    private final EdgeProperties properties;
    private final FileFallbackStore store;
    private final SmartShipMetrics metrics;

    /** 毒行记次（id/行内容 → 失败次数），重启清空（重启后值得再试）。 */
    private final java.util.Map<String, Integer> rowAttempts = new java.util.HashMap<>();

    @Scheduled(
            fixedDelayString = "${smartship.edge.persist.replay-interval-ms:30000}",
            initialDelayString = "${smartship.edge.persist.replay-interval-ms:30000}"
    )
    public void replay() {
        if (!properties.getCollect().getPersist().isReplayEnabled()) {
            return;
        }
        int budget = Math.max(1, properties.getCollect().getPersist().getReplayBatchSize());
        try {
            budget = replayDbTable(budget);
            List<FileFallbackStore.PendingFile> files = store.pendingFiles();
            for (FileFallbackStore.PendingFile file : files) {
                if (budget <= 0) {
                    return;
                }
                budget = replayFile(file, budget);
            }
        } catch (Exception e) {
            log.warn("[Fallback-Replay] 回放周期异常（下轮重试）: {}", e.getMessage());
        }
    }

    /**
     * 回放 DB 兜底表（按 id 升序，共享预算）。表不存在（如极老库）直接跳过；
     * 首个回放失败即停（DB 可能仍不可用），毒行跳过继续。
     */
    int replayDbTable(int budget) {
        final List<DbRow> rows;
        try {
            rows = jdbcTemplate.query(
                    "SELECT id, stream, mmsi, payload FROM zncb_failed_writes"
                            + " ORDER BY id ASC LIMIT ?",
                    (rs, n) -> new DbRow(rs.getLong("id"), rs.getString("stream"),
                            rs.getString("mmsi"), rs.getString("payload")),
                    Math.max(1, budget));
        } catch (Exception e) {
            log.debug("[Fallback-Replay] 兜底表不可读，跳过 DB 阶段: {}", e.getMessage());
            return budget;
        }
        int used = 0;
        for (DbRow row : rows) {
            if (used >= budget) {
                break;
            }
            // key 仅由 id 派生：超限行直接跳过，不再重复解析（解析本身每轮也有开销，
            // 且未知流分支会重复计数丢失指标）。
            String key = "db:" + row.id();
            if (overAttempted(key)) {
                continue;
            }
            final FileFallbackStore.ParsedLine parsed;
            try {
                parsed = FileFallbackStore.parseLine(row.payload());
            } catch (Exception e) {
                if (noteAttempt(key)) {
                    log.warn("[Fallback-Replay] 兜底表毒行跳过保留 (id={})", row.id());
                }
                continue;
            }
            if (NmeaDataPersistenceService.sqlForStream(parsed.stream()) == null) {
                if (noteAttempt(key)) {
                    log.warn("[Fallback-Replay] 兜底表未知流跳过保留 (id={}, stream={})",
                            row.id(), parsed.stream());
                }
                continue;
            }
            if (replayParsed(parsed.stream(), parsed.args())) {
                jdbcTemplate.update("DELETE FROM zncb_failed_writes WHERE id = ?", row.id());
                rowAttempts.remove(key);
                used++;
            } else {
                noteAttempt(key);
                break;
            }
        }
        return budget - used;
    }

    private record DbRow(long id, String stream, String mmsi, String payload) {
    }

    /** 记一次失败；返回 true 表示刚达到上限（调用方打一次日志）。 */
    private boolean noteAttempt(String key) {
        int n = rowAttempts.getOrDefault(key, 0) + 1;
        rowAttempts.put(key, n);
        return n == MAX_ROW_ATTEMPTS;
    }

    private boolean overAttempted(String key) {
        return rowAttempts.getOrDefault(key, 0) >= MAX_ROW_ATTEMPTS;
    }

    /** 通用单行回放：成功/失败计数，失败不抛。 */
    private boolean replayParsed(String stream, List<FileFallbackStore.TypedArg> args) {
        String sql = NmeaDataPersistenceService.sqlForStream(stream);
        if (sql == null) {
            log.warn("[Fallback-Replay] 未知流 {}，跳过（计数丢失）", stream);
            if (metrics != null) {
                metrics.recordFallbackDropped(1);
            }
            return true;
        }
        try {
            Object[] jdbcArgs = args.stream().map(FileFallbackStore::decode).toArray();
            jdbcTemplate.update(sql, jdbcArgs);
            if (metrics != null) {
                metrics.recordFallbackReplay(true);
            }
            return true;
        } catch (Exception e) {
            if (metrics != null) {
                metrics.recordFallbackReplay(false);
            }
            log.warn("[Fallback-Replay] 回放失败保留现场: stream={}, err={}",
                    stream, e.getMessage());
            return false;
        }
    }

    /** 回放单个文件，返回剩余预算；失败即停。 */
    int replayFile(FileFallbackStore.PendingFile file, int budget) throws IOException {
        FileFallbackStore.ReadResult read = store.readAll(file);
        if (read.badLines() > 0 && metrics != null) {
            metrics.recordFallbackDropped(read.badLines());
        }
        List<String> remaining = new ArrayList<>();
        int used = 0;
        for (FileFallbackStore.ReplayRecord record : read.records()) {
            if (used >= budget) {
                remaining.add(record.rawLine());
                continue;
            }
            if (replayOne(record)) {
                used++;
            } else {
                remaining.add(record.rawLine());
                // 首败即停：DB 可能仍不可用，保留现场下轮再试
                int idx = read.records().indexOf(record);
                for (int i = idx + 1; i < read.records().size(); i++) {
                    remaining.add(read.records().get(i).rawLine());
                }
                break;
            }
        }
        // 预算耗尽但文件还有剩余：上面循环已把剩余行全部收集（used>=budget 分支）
        store.rewrite(file, remaining);
        return budget - used;
    }

    private boolean replayOne(FileFallbackStore.ReplayRecord record) {
        return replayParsed(record.stream(), record.args());
    }
}
