package com.smartship.edge.persistence;

/** Named row property lets MyBatis address a batch cell as rows[i].args[j]. */
public record TelemetryWrite(Object[] args) {}
