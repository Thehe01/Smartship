package com.smartship.edge.collector.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConfigDevice {
    private Long id;
    private Long deviceCode;
    private String deviceName;
    private String deviceType;
    private BaseInfoVO baseInfo;
}
