package com.smartship.edge;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ShipSchemaIsolationTest {

    @Test
    @DisplayName("测试单船库 Schema 自包含：无注册表、无分船路由残留")
    void testSchemaIsolation() throws IOException {
        ClassPathResource shipSchemaRes = new ClassPathResource("schema/ship-schema.sql");
        assertTrue(shipSchemaRes.exists(), "本船库 schema/ship-schema.sql 必须存在");

        String shipSql = new String(shipSchemaRes.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        // 单船模式：绝不能包含分船注册表
        assertFalse(shipSql.contains("ship_database_registry"),
                "单船库 schema/ship-schema.sql 绝不能包含 ship_database_registry 注册表");

        // 验证包含完整的 5 大传感器遥测表与游标断点表
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_gps_data"), "必须包含 GPS 时序表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_wind_data"), "必须包含风速风向时序表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_depth_data"), "必须包含水深时序表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_rudder_data"), "必须包含舵角时序表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_engine_data"), "必须包含主机 Modbus 遥测表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_upload_cursor"), "必须包含本地上传游标断点表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_failed_writes"), "必须包含本地写入兜底表");

        // 回放幂等列：5 张时序表必须全部携带 replay_id + 唯一约束
        for (String table : new String[]{"gps", "wind", "depth", "rudder", "engine"}) {
            assertTrue(shipSql.contains("UNIQUE KEY uk_" + table + "_replay_id (replay_id)"),
                    table + " 表必须有 replay_id 唯一约束");
        }
        assertTrue(shipSql.contains("UNIQUE KEY uk_failed_replay_id (replay_id)"),
                "兜底表必须有 replay_id 唯一约束");

        // 分船注册表文件必须已删除（单船模式无主认证库）
        ClassPathResource authSchemaRes = new ClassPathResource("schema/auth-schema.sql");
        assertFalse(authSchemaRes.exists(), "单船模式下 schema/auth-schema.sql 必须已删除");

        // 验证旧的混合文件已被彻底删除
        ClassPathResource legacyRes = new ClassPathResource("schema/serialdata-schema.sql");
        assertFalse(legacyRes.exists(), "旧的混合表结构文件 schema/serialdata-schema.sql 必须已被彻底删除，拒绝兼容回退");
    }
}
