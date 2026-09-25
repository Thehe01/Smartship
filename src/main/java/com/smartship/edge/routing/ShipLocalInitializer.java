package com.smartship.edge.routing;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.config.MmsiPersistence;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

/**
 * 单船本地库初始化服务（无分船路由）。
 *
 * <p>船端只存本船数据：唯一数据源即 Spring 默认单数据源（本地本船库，
 * 表结构见 {@code schema/ship-schema.sql}）。本服务只做两件事：
 * 1) 确保本地时序表结构存在；2) 置位 {@code schemaReady} 并持久化 MMSI。
 * 方法签名刻意与旧 {@code ShipAutoRegisterService#ensureRegistered} 保持一致，
 * 以最小化 {@code NmeaParser} 改动。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShipLocalInitializer {

    private final JdbcTemplate jdbcTemplate;
    private final EdgeProperties properties;
    private volatile String preparedMmsi;

    public synchronized void ensureRegistered(String mmsi) {
        if (mmsi == null || mmsi.isEmpty()) {
            properties.setSchemaReady(false);
            return;
        }
        if (mmsi.equals(preparedMmsi) && properties.isSchemaReady()) {
            return;
        }

        properties.setMmsi(mmsi);
        if (properties.getShipId() == null || properties.getShipId().isEmpty()) {
            properties.setShipId(mmsi);
        }
        try {
            executeShipSchema(jdbcTemplate);
            ensureReplayIdColumns(jdbcTemplate);
            properties.setSchemaReady(true);
            preparedMmsi = mmsi;
            MmsiPersistence.write(mmsi);
            log.info("[LocalInit] 本船库已就绪: mmsi={}, shipId={}", mmsi, properties.getShipId());
        } catch (Exception e) {
            properties.setSchemaReady(false);
            preparedMmsi = null;
            log.warn("[LocalInit] 初始化本船库时序表失败: mmsi={}, err={}", mmsi, e.getMessage());
        }
    }

    private void executeShipSchema(JdbcTemplate jt) {
        try {
            ClassPathResource resource = new ClassPathResource("schema/ship-schema.sql");
            if (!resource.exists()) {
                throw new IllegalStateException("缺少 schema/ship-schema.sql，无法初始化本船时序库结构");
            }
            String sql = new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            String cleaned = sql.replaceAll("(?m)^--.*$", "");
            for (String statement : cleaned.split(";")) {
                String trimmed = statement.trim();
                if (!trimmed.isEmpty()) {
                    jt.execute(trimmed);
                }
            }
            log.debug("[LocalInit] 本船库时序表结构检查/自建完成");
        } catch (Exception e) {
            throw new IllegalStateException("时序表结构初始化异常: " + e.getMessage(), e);
        }
    }

    /**
     * 存量库迁移：给 5 张时序表 + 兜底表补 {@code replay_id} 列与唯一约束
     * （新库建表已自带）。
     * <p>{@code CREATE TABLE IF NOT EXISTS} 不会给老表加列，不迁则正常 INSERT
     * 因缺列失败。本方法失败即抛——调用方 {@link #ensureRegistered} 会置
     * {@code schemaReady=false} 并下轮重试，绝不在缺列状态下放行写入。
     * <p>约束名与 {@code schema/ship-schema.sql} 逐字一致（uk_gps_replay_id 等）；
     * 历史版本的长名重复索引（uk_zncb_*_replay_id）会被 best-effort 清理。
     */
    static void ensureReplayIdColumns(JdbcTemplate jt) {
        java.util.Map<String, String> constraints = new java.util.LinkedHashMap<>();
        constraints.put("zncb_gps_data", "uk_gps_replay_id");
        constraints.put("zncb_wind_data", "uk_wind_replay_id");
        constraints.put("zncb_depth_data", "uk_depth_replay_id");
        constraints.put("zncb_rudder_data", "uk_rudder_replay_id");
        constraints.put("zncb_engine_data", "uk_engine_replay_id");
        constraints.put("zncb_failed_writes", "uk_failed_replay_id");
        java.util.List<String> failed = new java.util.ArrayList<>();
        for (java.util.Map.Entry<String, String> e : constraints.entrySet()) {
            String table = e.getKey();
            String canonical = e.getValue();
            if (!tableExists(jt, table)) {
                // 表本身不存在：建表是 executeShipSchema 的职责（先执行），迁移跳过即可。
                log.debug("[LocalInit] 存量表 {} 不存在，跳过该表迁移", table);
                continue;
            }
            // 历史版本曾用表全名建过等价唯一索引，先清掉，避免同列双 UNIQUE。
            dropQuietly(jt, table, "uk_" + table + "_replay_id");
            cleanupNonCanonicalUnique(jt, table, canonical);
            if (!columnExists(jt, table, "replay_id")) {
                try {
                    jt.execute("ALTER TABLE " + table
                            + " ADD COLUMN replay_id VARCHAR(64) NULL DEFAULT NULL");
                    log.info("[LocalInit] 存量表 {} 迁移成功: ADD COLUMN replay_id", table);
                } catch (Exception ex) {
                    // 元数据复核：列已在=幂等成功；表没了=跳过；其余一律真实失败。
                    // 绝不靠 "Duplicate" 文本判断——表内重复数据导致建 UNIQUE 失败时
                    // MySQL 同样报 Duplicate entry，必须失败而不能当“已存在”放行。
                    if (columnExists(jt, table, "replay_id")) {
                        log.debug("[LocalInit] 存量表 {} 列已存在，跳过", table);
                    } else if (!tableExists(jt, table)) {
                        log.debug("[LocalInit] 存量表 {} 已不存在，跳过", table);
                        continue;
                    } else {
                        log.warn("[LocalInit] 存量表 {} 加列失败: {}", table, ex.getMessage());
                        failed.add(table + ":replay_id column");
                        continue;
                    }
                }
            }
            if (canonicalUniqueExists(jt, table, canonical)) {
                continue;
            }
            try {
                jt.execute("ALTER TABLE " + table
                        + " ADD CONSTRAINT " + canonical + " UNIQUE (replay_id)");
                log.info("[LocalInit] 存量表 {} 迁移成功: ADD CONSTRAINT {}", table, canonical);
            } catch (Exception ex) {
                // 元数据复核：约束已在=幂等成功（并发/重跑）；表没了=跳过；
                // 其余一律真实失败——包括表内已有重复 replay_id 导致 UNIQUE 建不起来的情况。
                if (canonicalUniqueExists(jt, table, canonical)) {
                    log.debug("[LocalInit] 存量表 {} 约束 {} 已存在，跳过", table, canonical);
                } else if (!tableExists(jt, table)) {
                    log.debug("[LocalInit] 存量表 {} 已不存在，跳过", table);
                } else {
                    log.warn("[LocalInit] 存量表 {} 建唯一约束失败: {}", table, ex.getMessage());
                    failed.add(table + ":replay_id unique");
                }
            }
        }
        if (!failed.isEmpty()) {
            throw new IllegalStateException("replay_id migration failed: " + failed);
        }
    }

    /**
     * 结构存在性一律走元数据/INFORMATION_SCHEMA，不靠异常文本：
     * <ul>
     *   <li>表是否存在：{@code DatabaseMetaData#getTables}</li>
     *   <li>列是否存在：{@code DatabaseMetaData#getColumns}</li>
     *   <li>规范唯一约束是否存在：{@code INFORMATION_SCHEMA.TABLE_CONSTRAINTS}</li>
     * </ul>
     * 这样“表内已有重复 replay_id 导致建 UNIQUE 失败（MySQL Duplicate entry）”
     * 不会被误判成“约束已存在”而放行，而是复核元数据发现约束确实没建成，如实失败。
     */
    private static boolean tableExists(JdbcTemplate jt, String table) {
        try {
            Boolean found = jt.execute(
                    (org.springframework.jdbc.core.ConnectionCallback<Boolean>) con -> {
                        for (String pattern : new String[]{table, table.toUpperCase(
                                java.util.Locale.ROOT)}) {
                            try (java.sql.ResultSet rs = con.getMetaData()
                                    .getTables(null, null, pattern, new String[]{"TABLE"})) {
                                while (rs.next()) {
                                    String name = rs.getString("TABLE_NAME");
                                    if (name != null && name.equalsIgnoreCase(table)) {
                                        return true;
                                    }
                                }
                            }
                        }
                        return false;
                    });
            if (found == null) {
                // 回调没跑起来（未知），按“表在”处理照常尝试 DDL，靠复核判定成败。
                return true;
            }
            if (found) {
                return true;
            }
            return tryInformationSchemaTable(jt, table);
        } catch (Exception e) {
            // 元数据查不到时按“表在”处理，照常尝试 DDL，靠复核判定成败。
            log.debug("[LocalInit] 表存在性查询失败，按存在处理: {}", e.getMessage());
            return true;
        }
    }

    private static boolean tryInformationSchemaTable(JdbcTemplate jt, String table) {
        try {
            Long n = jt.queryForObject(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = '"
                            + table.toUpperCase(java.util.Locale.ROOT) + "'",
                    Long.class);
            if (n == null) {
                return true;
            }
            return n > 0;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static boolean columnExists(JdbcTemplate jt, String table, String column) {
        try {
            Boolean found = jt.execute(
                    (org.springframework.jdbc.core.ConnectionCallback<Boolean>) con -> {
                        for (String tPattern : new String[]{table, table.toUpperCase(
                                java.util.Locale.ROOT)}) {
                            try (java.sql.ResultSet rs = con.getMetaData()
                                    .getColumns(null, null, tPattern, "%")) {
                                while (rs.next()) {
                                    String name = rs.getString("COLUMN_NAME");
                                    if (name != null && name.equalsIgnoreCase(column)) {
                                        return true;
                                    }
                                }
                            }
                        }
                        return false;
                    });
            return Boolean.TRUE.equals(found);
        } catch (Exception e) {
            log.debug("[LocalInit] 列存在性查询失败，按不存在处理: {}", e.getMessage());
            return false;
        }
    }

    /** 规范名唯一约束是否存在（大小写不敏感）。查不到/异常一律按“不存在”处理。 */
    private static boolean canonicalUniqueExists(JdbcTemplate jt, String table, String canonical) {
        try {
            Long n = jt.queryForObject(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
                            + " WHERE UPPER(TABLE_NAME) = '"
                            + table.toUpperCase(java.util.Locale.ROOT) + "'"
                            + " AND UPPER(CONSTRAINT_NAME) = '"
                            + canonical.toUpperCase(java.util.Locale.ROOT) + "'"
                            + " AND CONSTRAINT_TYPE = 'UNIQUE'",
                    Long.class);
            if (n != null && n > 0) {
                return true;
            }
        } catch (Exception e) {
            log.debug("[LocalInit] 约束存在性查询失败，改用索引元数据复核: {}", e.getMessage());
        }
        // 兜底：索引元数据里找规范名（H2 后台索引名可能加后缀，这里只做精确复核的补充）。
        try {
            Boolean found = jt.execute(
                    (org.springframework.jdbc.core.ConnectionCallback<Boolean>) con -> {
                        try (java.sql.ResultSet rs = con.getMetaData()
                                .getIndexInfo(null, null, table, true, false)) {
                            while (rs.next()) {
                                String idx = rs.getString("INDEX_NAME");
                                if (idx != null && idx.equalsIgnoreCase(canonical)) {
                                    return true;
                                }
                            }
                        } catch (Exception ignored) {
                            // 某些驱动对大小写敏感，再试大写表名
                        }
                        try (java.sql.ResultSet rs = con.getMetaData().getIndexInfo(
                                null, null, table.toUpperCase(java.util.Locale.ROOT), true, false)) {
                            while (rs.next()) {
                                String idx = rs.getString("INDEX_NAME");
                                if (idx != null && idx.equalsIgnoreCase(canonical)) {
                                    return true;
                                }
                            }
                        }
                        return false;
                    });
            return Boolean.TRUE.equals(found);
        } catch (Exception e) {
            log.debug("[LocalInit] 索引元数据复核失败，按约束不存在处理: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Best-effort 清理列集合恰好只有 {@code replay_id} 的非规范名单列唯一索引
     * （历史长名等），避免同列双 UNIQUE。复合唯一索引（如未来可能出现的
     * {@code (replay_id, other_col)}）一律保留。找不到/删不掉一律忽略，
     * 不影响主流程。
     */
    private static void cleanupNonCanonicalUnique(JdbcTemplate jt, String table, String canonical) {
        try {
            java.util.List<String> extras = jt.execute(
                    (org.springframework.jdbc.core.ConnectionCallback<java.util.List<String>>) con -> {
                        java.util.Map<String, java.util.Set<String>> colsByIndex =
                                new java.util.LinkedHashMap<>();
                        for (String tPattern : new String[]{table, table.toUpperCase(
                                java.util.Locale.ROOT)}) {
                            try (java.sql.ResultSet rs = con.getMetaData()
                                    .getIndexInfo(null, null, tPattern, true, false)) {
                                while (rs.next()) {
                                    boolean nonUnique;
                                    try {
                                        nonUnique = rs.getBoolean("NON_UNIQUE");
                                    } catch (Exception ignored) {
                                        continue;
                                    }
                                    if (nonUnique) {
                                        continue;
                                    }
                                    String idx = rs.getString("INDEX_NAME");
                                    String col = rs.getString("COLUMN_NAME");
                                    if (idx == null || col == null) {
                                        continue;
                                    }
                                    colsByIndex.computeIfAbsent(idx,
                                            k -> new java.util.LinkedHashSet<>()).add(
                                            col.toLowerCase(java.util.Locale.ROOT));
                                }
                            } catch (Exception ignored) {
                                // 换下一种表名大小写再试
                            }
                        }
                        java.util.List<String> out = new java.util.ArrayList<>();
                        for (java.util.Map.Entry<String, java.util.Set<String>> en
                                : colsByIndex.entrySet()) {
                            if (en.getKey().equalsIgnoreCase(canonical)) {
                                continue;
                            }
                            // 仅清理“列集合恰好只有 replay_id”的单列唯一索引；
                            // 复合唯一索引（如 (replay_id, other_col)）必须保留。
                            // 用去重后的小写列集合判断：同一索引在两种表名大小写下
                            // 各返回一行时不会被误判成多列。
                            java.util.Set<String> cols = en.getValue();
                            if (cols.size() == 1 && cols.contains("replay_id")) {
                                out.add(en.getKey());
                            }
                        }
                        return out;
                    });
            if (extras == null) {
                return;
            }
            for (String idx : extras) {
                dropQuietly(jt, table, idx);
            }
        } catch (Exception e) {
            log.debug("[LocalInit] 非规范唯一索引清理跳过: {}", e.getMessage());
        }
    }

    /** 历史重名索引清理：存在即删，不存在/语法不支持一律忽略（绝不影响主流程）。 */
    private static void dropQuietly(JdbcTemplate jt, String table, String index) {
        try {
            jt.execute("ALTER TABLE " + table + " DROP INDEX " + index);
            log.info("[LocalInit] 存量表 {} 清理历史重复索引 {}", table, index);
            return;
        } catch (Exception ignored) {
            // 方言不支持或索引不存在，换第二种语法再试
        }
        try {
            jt.execute("ALTER TABLE " + table + " DROP CONSTRAINT " + index);
            log.info("[LocalInit] 存量表 {} 清理历史重复索引 {}", table, index);
        } catch (Exception ignored) {
            // 不存在即无事可做
        }
    }
}
