package com.smartship.edge;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ShipSchemaIsolationTest {

    @Test
    @DisplayName("测试分船库 Schema 与主认证库 Schema 物理彻底隔离")
    void testSchemaIsolation() throws IOException {
        ClassPathResource shipSchemaRes = new ClassPathResource("schema/ship-schema.sql");
        assertTrue(shipSchemaRes.exists(), "分船库 schema/ship-schema.sql 必须存在");

        String shipSql = new String(shipSchemaRes.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        // 验证分船库不包含主注册表
        assertFalse(shipSql.contains("ship_database_registry"),
                "分船库 schema/ship-schema.sql 绝不能包含主认证库的 ship_database_registry 注册表");

        // 验证分船库包含完整的 5 大传感器遥测表与游标断点表
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_gps_data"), "必须包含 GPS 时序表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_wind_data"), "必须包含风速风向时序表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_depth_data"), "必须包含水深时序表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_rudder_data"), "必须包含舵角时序表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_engine_data"), "必须包含主机 Modbus 遥测表");
        assertTrue(shipSql.contains("CREATE TABLE IF NOT EXISTS zncb_upload_cursor"), "必须包含本地上传游标断点表");

        // 验证主认证库 schema
        ClassPathResource authSchemaRes = new ClassPathResource("schema/auth-schema.sql");
        assertTrue(authSchemaRes.exists(), "主认证库 schema/auth-schema.sql 必须存在");

        String authSql = new String(authSchemaRes.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(authSql.contains("CREATE TABLE IF NOT EXISTS ship_database_registry"),
                "主认证库 schema/auth-schema.sql 必须包含 ship_database_registry 注册表");
        assertFalse(authSql.contains("CREATE TABLE IF NOT EXISTS zncb_gps_data"),
                "主认证库不应混杂分船业务时序表");
    }
}
