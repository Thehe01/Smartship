package com.smartship.edge.collector.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConfigAlarm {
    private Long id;
    private String alarmName;
    private String alarmCode;
    private Integer bitIndex;
    private String alarmLevel;
}
