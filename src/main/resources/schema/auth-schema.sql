-- ======================================================================
-- 主认证与分船注册元数据表（位于主库，供 ShipDataSourceManager 进行动态分船数据库解析）
-- ======================================================================
CREATE TABLE IF NOT EXISTS ship_database_registry (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ship_id VARCHAR(64) NOT NULL UNIQUE COMMENT '船舶业务编号',
    mmsi VARCHAR(32) NOT NULL UNIQUE COMMENT '九位国际海事识别码 MMSI',
    database_name VARCHAR(64) NOT NULL COMMENT '对应船舶独立数据库名',
    host VARCHAR(128) DEFAULT 'localhost',
    port INT DEFAULT 3306,
    username VARCHAR(64) DEFAULT 'root',
    password VARCHAR(64) DEFAULT '123456',
    enabled TINYINT(1) DEFAULT 1 COMMENT '是否启用 1-启用 0-禁用',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分船数据库元数据注册表';

-- 初始示例船舶数据
INSERT INTO ship_database_registry (ship_id, mmsi, database_name, host, port, username, password, enabled)
VALUES ('SHIP_DEMO_01', '413999999', 'zncb_ship_413999999', 'localhost', 3306, 'root', '123456', 1)
ON DUPLICATE KEY UPDATE enabled = 1;
