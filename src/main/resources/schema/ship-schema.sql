-- ======================================================================
-- 船舶独立业务数据库表结构（由 ShipAutoRegisterService 自动向目标船库刷入）
-- 注意：本文件仅包含分船业务遥测表与游标表，主库元数据位于 auth-schema.sql
-- ======================================================================

-- 1. 导航 GPS 综合流表
CREATE TABLE IF NOT EXISTS zncb_gps_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ship_id VARCHAR(64) NOT NULL COMMENT '船舶编号',
    mmsi VARCHAR(32) NOT NULL COMMENT 'MMSI',
    sentence_type VARCHAR(10),
    source VARCHAR(32) DEFAULT 'serial',
    timestamp DATETIME,
    latitude DECIMAL(10,7) COMMENT '十进制度数纬度 (-90 ~ 90)',
    longitude DECIMAL(10,7) COMMENT '十进制度数经度 (-180 ~ 180)',
    speed_knots DECIMAL(8,3) COMMENT '航速 (节)',
    course_over_ground DECIMAL(8,3) COMMENT '对地航向 (度)',
    heading_true DECIMAL(8,3) COMMENT '真航向 (度)',
    heading_magnetic DECIMAL(8,3) COMMENT '磁航向 (度)',
    magnetic_variation DECIMAL(8,3),
    altitude_m DECIMAL(8,2) COMMENT '海拔高度 (米)',
    satellites INT COMMENT '卫星数量',
    hdop DECIMAL(5,2) COMMENT '水平精度因子',
    position_quality INT,
    gps_status VARCHAR(10) COMMENT '定位状态 A-有效 V-警告',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ship_ts (ship_id, timestamp),
    INDEX idx_mmsi (mmsi)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='GPS 综合导航数据表';

-- 2. 风速风向表
CREATE TABLE IF NOT EXISTS zncb_wind_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ship_id VARCHAR(64) NOT NULL,
    mmsi VARCHAR(32) NOT NULL,
    sentence_type VARCHAR(10),
    source VARCHAR(32) DEFAULT 'serial',
    timestamp DATETIME,
    apparent_wind_angle DECIMAL(8,3) COMMENT '相对风角',
    apparent_wind_speed DECIMAL(8,3) COMMENT '相对风速',
    true_wind_angle DECIMAL(8,3) COMMENT '真风角',
    true_wind_direction DECIMAL(8,3) COMMENT '真风向',
    true_wind_speed DECIMAL(8,3) COMMENT '真风速',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ship_ts (ship_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='风速风向数据表';

-- 3. 水深测深表
CREATE TABLE IF NOT EXISTS zncb_depth_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ship_id VARCHAR(64) NOT NULL,
    mmsi VARCHAR(32) NOT NULL,
    sentence_type VARCHAR(10),
    source VARCHAR(32) DEFAULT 'serial',
    timestamp DATETIME,
    depth_m DECIMAL(8,2) COMMENT '水深 (米)',
    transducer_offset_m DECIMAL(8,2) COMMENT '换能器吃水偏移',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ship_ts (ship_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='水深测深数据表';

-- 4. 舵角表
CREATE TABLE IF NOT EXISTS zncb_rudder_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ship_id VARCHAR(64) NOT NULL,
    mmsi VARCHAR(32) NOT NULL,
    sentence_type VARCHAR(10),
    source VARCHAR(32) DEFAULT 'serial',
    timestamp DATETIME,
    rudder_angle DECIMAL(8,3) COMMENT '舵角 (度)',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ship_ts (ship_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='舵角传感器数据表';

-- 5. 主机/动力机舱 Modbus 运行参数表
CREATE TABLE IF NOT EXISTS zncb_engine_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ship_id VARCHAR(64) NOT NULL,
    mmsi VARCHAR(32) NOT NULL,
    slave_id INT COMMENT '从站 ID',
    device_id VARCHAR(50),
    protocol VARCHAR(20) DEFAULT 'MODBUS_TCP',
    timestamp DATETIME,
    rpm DECIMAL(8,2) COMMENT '转速',
    coolant_temp DECIMAL(8,2) COMMENT '冷却水温 (℃)',
    lube_oil_press DECIMAL(8,4) COMMENT '滑油压力 (MPa)',
    fuel_press DECIMAL(8,4) COMMENT '燃油压力 (MPa)',
    exhaust_temp DECIMAL(8,2) COMMENT '排气温度 (℃)',
    tc_air_press DECIMAL(8,4) COMMENT '增压器进气压力',
    start_air_press DECIMAL(8,4) COMMENT '起动空气压力',
    bearing_temp DECIMAL(8,2) COMMENT '轴承温度 (℃)',
    battery_volt DECIMAL(8,2) COMMENT '电瓶电压 (V)',
    running_hours INT COMMENT '运行累计小时数',
    status INT COMMENT '运行状态',
    alarm_bits1 INT COMMENT '故障标志位1',
    alarm_bits2 INT COMMENT '故障标志位2',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ship_ts (ship_id, timestamp),
    INDEX idx_slave (slave_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='主机 Modbus 遥测数据表';

-- 6. [模块四] 本地上传游标断点记录表
CREATE TABLE IF NOT EXISTS zncb_upload_cursor (
    stream_name VARCHAR(64) NOT NULL,
    partition_key VARCHAR(64) NOT NULL DEFAULT '',
    last_uploaded_id BIGINT NOT NULL DEFAULT 0 COMMENT '增量流最后成功推送的主键 ID',
    last_uploaded_time DATETIME NULL COMMENT '快照流最后成功推送的修改时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_name, partition_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='本地到云端上报游标断点表';
