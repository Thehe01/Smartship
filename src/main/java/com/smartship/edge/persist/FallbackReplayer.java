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
 * <p>顺序：先 DB 兜底表（zncb_failed_writes，id 游标分页），再磁盘 spool
 * （最老文件优先），共享单轮预算，避免长停机后打爆 DB。
 *
 * <p>幂等性：
 * <ul>
 *   <li>DB 行：单行本地事务（INSERT 主表 + DELETE 兜底行原子提交），崩溃要么全有
 *   要么全无——不存在“写完主表、没删兜底行”的重复窗口；</li>
 *   <li>文件行：每成功一行立即原子回写进度，崩溃最多重放当前行（单行窗口，
 *   下游 at-least-once + 岸端幂等吸收）。</li>
 * </ul>
 *
 * <p>失败分级（绝不错杀正常数据）：
 * <ul>
 *   <li>解析失败 / 未知流 / 确定性数据错误（违反约束、超长、坏语法等）→ 毒行记次，
 *   超 {@code MAX_ROW_ATTEMPTS} 后跳过保留（行保留供人工审计），游标继续推进，
 *   永不饿死后面的正常行；</li>
 *   <li>瞬时故障（拿不到连接、连接异常、锁等待超时、死锁等）→ <b>不计数</b>，
 *   本轮即停、下轮原样重试。MySQL 长时间故障也不会把合法数据熬成毒行。</li>
 * </ul>
 * 未知异常一律按瞬时处理：宁可多重试一轮，绝不错杀。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FallbackReplayer {

    /** 单行内存重试上限：超限后跳过（行保留，供人工审计）。 */
    static final int MAX_ROW_ATTEMPTS = 5;
    /** 单轮 DB 分页上限：毒行再多也有界终止。 */
    private static final int MAX_DB_PAGES = 10;

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
     * 回放 DB 兜底表（id 游标分页，固定小页）。表不存在（如极老库）直接跳过；
     * 瞬时 DB 故障即停（下轮原样重试，不记毒）；确定性坏行记次，超限跳过继续——
     * 游标按 id 单调推进，毒行再多也不饿死后面的正常行；只有成功回放才消耗 budget。
     */
    int replayDbTable(int budget) {
        int replayed = 0;
        long cursor = 0;
        final int pageSize = 100;
        boolean dbDown = false;
        while (replayed < budget && !dbDown) {
            final List<DbRow> rows;
            try {
                rows = jdbcTemplate.query(
                        "SELECT id, stream, mmsi, replay_id, payload FROM zncb_failed_writes"
                                + " WHERE id > ? ORDER BY id ASC LIMIT ?",
                        (rs, n) -> new DbRow(rs.getLong("id"), rs.getString("stream"),
                                rs.getString("mmsi"), rs.getString("replay_id"),
                                rs.getString("payload")),
                        cursor, pageSize);
            } catch (Exception e) {
                log.debug("[Fallback-Replay] 兜底表不可读，跳过 DB 阶段: {}", e.getMessage());
                break;
            }
            if (rows.isEmpty()) {
                break;
            }
            for (DbRow row : rows) {
                cursor = row.id();
                if (replayed >= budget) {
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
                // 幂等键以兜底表列为准（写入时与 payload 同值），老诊断行列为空才退到 JSON。
                String rid = hasText(row.replayId()) ? row.replayId() : parsed.replayId();
                RowOutcome outcome = replayDbRow(row, parsed, rid);
                if (outcome == RowOutcome.OK) {
                    rowAttempts.remove(key);
                    replayed++;
                } else if (outcome == RowOutcome.TRANSIENT_FAIL) {
                    // 瞬时故障：不记毒，本轮即停，下轮原样重试。
                    dbDown = true;
                    break;
                } else {
                    // 确定性坏行：记毒，超限后跳过保留；DB 既然可用就继续扫后面的行。
                    if (noteAttempt(key)) {
                        log.warn("[Fallback-Replay] 兜底表坏行跳过保留 (id={}, stream={})",
                                row.id(), parsed.stream());
                    }
                }
            }
        }
        return budget - replayed;
    }

    private record DbRow(long id, String stream, String mmsi, String replayId, String payload) {
    }

    /**
     * 单行 DB 回放：INSERT 主表 + DELETE 兜底行同一本地事务原子提交。
     * <p>崩溃要么全有要么全无——消灭“写完主表、没删兜底行”导致的重复窗口
     * （重复行的自增 id 不同会导致岸端 msg_id 不同而无法去重）。
     * <p>返回值区分瞬时故障与确定性坏行：前者调用方不记毒（下轮重试），
     * 后者调用方记毒（超限跳过，避免一坏行永久堵住队列）。
     */
    private RowOutcome replayDbRow(
            DbRow row, FileFallbackStore.ParsedLine parsed, String replayId) {
        String sql = NmeaDataPersistenceService.sqlForStream(parsed.stream());
        Object[] jdbcArgs = withReplayId(parsed, replayId);
        javax.sql.DataSource ds = jdbcTemplate.getDataSource();
        if (ds == null) {
            if (metrics != null) {
                metrics.recordFallbackReplay(false);
            }
            log.warn("[Fallback-Replay] 无 DataSource，保留现场下轮重试: stream={}",
                    parsed.stream());
            return RowOutcome.TRANSIENT_FAIL;
        }
        try (java.sql.Connection con = ds.getConnection()) {
            con.setAutoCommit(false);
            try {
                org.springframework.jdbc.datasource.SingleConnectionDataSource scf =
                        new org.springframework.jdbc.datasource.SingleConnectionDataSource(con, true);
                JdbcTemplate tx = new JdbcTemplate(scf);
                try {
                    tx.update(sql, jdbcArgs);
                } catch (org.springframework.dao.DuplicateKeyException dup) {
                    // 崩溃重试：主表行已在，唯一约束吸收重复（与 ODKU 等价，
                    // 且 Spring 在 MySQL/H2 下都翻译为该异常，单路径可测）。
                    log.info("[Fallback-Replay] 重复回放被唯一约束吸收: stream={}, replay_id={}",
                            parsed.stream(), parsed.replayId());
                }
                tx.update("DELETE FROM zncb_failed_writes WHERE id = ?", row.id());
                con.commit();
                if (metrics != null) {
                    metrics.recordFallbackReplay(true);
                }
                return RowOutcome.OK;
            } catch (Exception e) {
                rollbackQuietly(con);
                if (metrics != null) {
                    metrics.recordFallbackReplay(false);
                }
                if (isDeterministicFailure(e)) {
                    log.warn("[Fallback-Replay] 坏行保留现场（记毒）: stream={}, err={}",
                            parsed.stream(), e.getMessage());
                    return RowOutcome.DATA_FAIL;
                }
                log.warn("[Fallback-Replay] 瞬时故障保留现场（不记毒，下轮重试）: stream={}, err={}",
                        parsed.stream(), e.getMessage());
                return RowOutcome.TRANSIENT_FAIL;
            }
        } catch (Exception e) {
            if (metrics != null) {
                metrics.recordFallbackReplay(false);
            }
            log.warn("[Fallback-Replay] 回放连接失败保留现场: stream={}, err={}",
                    parsed.stream(), e.getMessage());
            return RowOutcome.TRANSIENT_FAIL;
        }
    }

    /** 单行回放结果。 */
    private enum RowOutcome {
        OK,
        TRANSIENT_FAIL,
        DATA_FAIL
    }

    /**
     * 确定性数据错误（行本身坏，重试无用）→ 调用方可记毒；
     * 其余一律按瞬时故障处理（宁可多重试一轮，绝不错杀正常数据）。
     * <p>判定只认两类积极信号：Spring 的数据错误异常族、
     * SQLState 数据异常（22）/完整性约束（23）。{@code DuplicateKeyException}
     * 明确不算（调用方在更早位置已吸收为成功）。
     */
    static boolean isDeterministicFailure(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur instanceof org.springframework.dao.DuplicateKeyException) {
                return false;
            }
            if (cur instanceof org.springframework.dao.DataIntegrityViolationException
                    || cur instanceof org.springframework.jdbc.BadSqlGrammarException
                    || cur instanceof
                    org.springframework.dao.InvalidDataAccessResourceUsageException
                    || cur instanceof org.springframework.dao.TypeMismatchDataAccessException) {
                return true;
            }
            if (cur instanceof java.sql.SQLException se) {
                String state = se.getSQLState();
                if (state != null && (state.startsWith("22") || state.startsWith("23"))) {
                    return true;
                }
            }
            if (cur.getCause() == cur) {
                break;
            }
        }
        return false;
    }

    private static void rollbackQuietly(java.sql.Connection con) {
        try {
            con.rollback();
        } catch (Exception ignored) {
            // 回滚失败不掩盖原始异常
        }
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

    /** 通用单行回放：成功/失败计数，失败不抛（重复由主表 replay_id 唯一约束吸收）。 */
    private boolean replayParsed(
            String stream, String replayId, List<FileFallbackStore.TypedArg> args) {
        String sql = NmeaDataPersistenceService.sqlForStream(stream);
        if (sql == null) {
            log.warn("[Fallback-Replay] 未知流 {}，跳过（计数丢失）", stream);
            if (metrics != null) {
                metrics.recordFallbackDropped(1);
            }
            return true;
        }
        try {
            Object[] decoded = args.stream().map(FileFallbackStore::decode).toArray();
            Object[] jdbcArgs = java.util.Arrays.copyOf(decoded, decoded.length + 1);
            jdbcArgs[decoded.length] = replayId;
            jdbcTemplate.update(sql, jdbcArgs);
            if (metrics != null) {
                metrics.recordFallbackReplay(true);
            }
            return true;
        } catch (org.springframework.dao.DuplicateKeyException dup) {
            // 文件重放的崩溃重试：同上，唯一约束吸收。
            log.info("[Fallback-Replay] 重复回放被唯一约束吸收: stream={}, replay_id={}",
                    stream, replayId);
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

    private Object[] withReplayId(FileFallbackStore.ParsedLine parsed, String replayId) {
        Object[] decoded =
                parsed.args().stream().map(FileFallbackStore::decode).toArray();
        Object[] jdbcArgs = java.util.Arrays.copyOf(decoded, decoded.length + 1);
        jdbcArgs[decoded.length] = replayId;
        return jdbcArgs;
    }

    private static boolean hasText(String s) {
        return s != null && !s.isBlank();
    }

    /**
     * 回放单个文件，返回剩余预算；失败即停。
     * <p>每成功一行立即原子回写进度（文件恒等于“未处理行”）：崩溃最多重放当前行，
     * 把重复窗口从“整文件”压缩到“单行”（残余窗口由下游 at-least-once 吸收）。
     */
    int replayFile(FileFallbackStore.PendingFile file, int budget) throws IOException {
        FileFallbackStore.ReadResult read = store.readAll(file);
        if (read.badLines() > 0 && metrics != null) {
            metrics.recordFallbackDropped(read.badLines());
        }
        List<FileFallbackStore.ReplayRecord> records = read.records();
        if (records.isEmpty()) {
            store.rewrite(file, List.of());
            return budget;
        }
        List<String> raws = new ArrayList<>(records.size());
        for (FileFallbackStore.ReplayRecord r : records) {
            raws.add(r.rawLine());
        }
        int used = 0;
        int i = 0;
        for (; i < records.size() && used < budget; i++) {
            if (!replayOne(records.get(i))) {
                break;
            }
            used++;
            store.rewrite(file, new ArrayList<>(raws.subList(i + 1, raws.size())));
        }
        // 不变量：退出时文件恒等于 raws[i..]（成功步已回写；首行失败/预算耗尽时
        // 文件本就是原文，无需额外回写）。
        return budget - used;
    }

    private boolean replayOne(FileFallbackStore.ReplayRecord record) {
        return replayParsed(record.stream(), record.replayId(), record.args());
    }
}
