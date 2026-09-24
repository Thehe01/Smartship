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
 * 兜底回放器：周期性把磁盘 spool 重写入主表。
 *
 * <p>语义：
 * <ul>
 *   <li>文件最老优先、文件内按行顺序回放，单轮不超过
 *   {@code persist.replay-batch-size} 行，避免长停机后打爆 DB；</li>
 *   <li>遇到第一条回放失败即停（保留剩余行下轮再试），避免热循环刷日志；</li>
 *   <li>全部成功的文件直接删除；部分成功则原子回写剩余行；</li>
 *   <li>回放成功/失败分别计数，坏行跳过计数（spool 写时已是合法 JSON，坏行只可能
 *   来自磁盘损坏）。</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FallbackReplayer {

    private final JdbcTemplate jdbcTemplate;
    private final EdgeProperties properties;
    private final FileFallbackStore store;
    private final SmartShipMetrics metrics;

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
        String sql = NmeaDataPersistenceService.sqlForStream(record.stream());
        if (sql == null) {
            log.warn("[Fallback-Replay] 未知流 {}，跳过（计数丢失）", record.stream());
            if (metrics != null) {
                metrics.recordFallbackDropped(1);
            }
            return true;
        }
        try {
            Object[] args = record.args().stream().map(FileFallbackStore::decode).toArray();
            jdbcTemplate.update(sql, args);
            if (metrics != null) {
                metrics.recordFallbackReplay(true);
            }
            return true;
        } catch (Exception e) {
            if (metrics != null) {
                metrics.recordFallbackReplay(false);
            }
            log.warn("[Fallback-Replay] 回放失败保留现场: stream={}, err={}",
                    record.stream(), e.getMessage());
            return false;
        }
    }
}
