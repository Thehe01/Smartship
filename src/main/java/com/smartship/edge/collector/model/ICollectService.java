package com.smartship.edge.collector.model;

public interface ICollectService {
    /**
     * 更新设备运行状态
     * @param device 设备信息
     * @param status 状态码 (如 "1"-正常在线, "2"-离线/异常)
     */
    void updateDeviceStatus(ConfigDevice device, String status);
}
