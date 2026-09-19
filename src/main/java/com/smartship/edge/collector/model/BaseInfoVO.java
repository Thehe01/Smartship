package com.smartship.edge.collector.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BaseInfoVO {
    private String ip;
    private Integer port;
    private Integer connectIntervalTime; // 心跳/超时检查间隔 (ms)
    private Integer queryLength;         // 下发查询指令长度
    private Integer frameLength;         // 响应数据帧长度
    private Long intervalTime;           // 采集轮询周期 (ms)
}
