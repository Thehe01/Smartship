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
        java.util.Set<String> existing = existingTables(jt);
        java.util.List<String> failed = new java.util.ArrayList<>();
        for (java.util.Map.Entry<String, String> e : constraints.entrySet()) {
            String table = e.getKey();
            if (existing != null
                    && !existing.contains(table.toUpperCase(java.util.Locale.ROOT))) {
                // 表本身不存在：建表是 executeShipSchema 的职责（先执行），迁移跳过即可。
                log.debug("[LocalInit] 存量表 {} 不存在，跳过该表迁移", table);
                continue;
            }
            // 历史版本曾用表全名建过等价唯一索引，先清掉，避免同列双 UNIQUE。
            dropQuietly(jt, table, "uk_" + table + "_replay_id");
            if (!execOk(jt, table,
                    "ALTER TABLE " + table
                            + " ADD COLUMN replay_id VARCHAR(64) NULL DEFAULT NULL")) {
                failed.add(table + ":replay_id column");
            }
            if (!execOk(jt, table,
                    "ALTER TABLE " + table
                            + " ADD CONSTRAINT " + e.getValue() + " UNIQUE (replay_id)")) {
                failed.add(table + ":replay_id unique");
            }
        }
        if (!failed.isEmpty()) {
            throw new IllegalStateException("replay_id migration failed: " + failed);
        }
    }

    /** 已存在表名（大写）；查不到时返回 null（调用方照常尝试 DDL，靠异常判定）。 */
    private static java.util.Set<String> existingTables(JdbcTemplate jt) {
        try {
            return jt.execute((org.springframework.jdbc.core.ConnectionCallback<java.util.Set<String>>) con -> {
                java.util.Set<String> names = new java.util.HashSet<>();
                try (java.sql.ResultSet rs = con.getMetaData()
                        .getTables(null, null, "%", new String[]{"TABLE"})) {
                    while (rs.next()) {
                        String name = rs.getString("TABLE_NAME");
                        if (name != null) {
                            names.add(name.toUpperCase(java.util.Locale.ROOT));
                        }
                    }
                }
                return names;
            });
        } catch (Exception e) {
            log.debug("[LocalInit] 表清单查询失败，逐表尝试迁移: {}", e.getMessage());
            return null;
        }
    }

    /** 重复执行安全：已存在直接忽略；返回 false 仅当真实异常。 */
    private static boolean execOk(JdbcTemplate jt, String table, String ddl) {
        try {
            jt.execute(ddl);
            log.info("[LocalInit] 存量表 {} 迁移成功: {}", table,
                    ddl.substring(0, Math.min(80, ddl.length())));
            return true;
        } catch (Exception e) {
            if (isDuplicateSignal(e) || isMissingObjectSignal(e)) {
                log.debug("[LocalInit] 存量表 {} 已有该结构/无需迁移，跳过", table);
                return true;
            }
            log.warn("[LocalInit] 存量表 {} 迁移失败: {}", table, e.getMessage());
            return false;
        }
    }

    /**
     * 重复列/约束：扫整条 cause 链（Spring 顶层 message 不带数据库原文，
     * 只看它永远匹配不上）+ SQLState 兜底。
     */
    private static boolean isDuplicateSignal(Exception e) {
        String state = sqlStateOf(e);
        if (state != null && state.startsWith("42S21")) {
            return true;
        }
        String text = fullChainText(e);
        return text.contains("Duplicate") || text.contains("duplicate")
                || text.contains("already exists") || text.contains("already Exists");
    }

    /** 缺表/缺对象：同上，扫整条链。建表是 schema 步骤的职责，迁移跳过。 */
    private static boolean isMissingObjectSignal(Exception e) {
        String state = sqlStateOf(e);
        if (state != null && state.startsWith("42S02")) {
            return true;
        }
        String lower = fullChainText(e).toLowerCase(java.util.Locale.ROOT);
        return lower.contains("not found") || lower.contains("doesn't exist");
    }

    /** 整条 cause 链文本（Spring 包装异常的原文藏在 cause 里）。 */
    private static String fullChainText(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            sb.append(String.valueOf(cur.getMessage())).append('\n');
            if (cur.getCause() == cur) {
                break;
            }
        }
        return sb.toString();
    }

    /** 沿 cause 链找底层 SQLException 的 SQLState。 */
    private static String sqlStateOf(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur instanceof java.sql.SQLException se && se.getSQLState() != null) {
                return se.getSQLState();
            }
            if (cur.getCause() == cur) {
                break;
            }
        }
        return null;
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
