package com.smartship.edge.collector.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConfigVariable {
    private Long id;
    private String variableName;
    private String variableCode;
    private Integer registerAddress;
    private String dataType;
    private Double scaleRatio;
}
