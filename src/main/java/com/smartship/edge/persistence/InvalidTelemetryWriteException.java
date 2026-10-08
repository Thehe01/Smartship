package com.smartship.edge.persistence;

/** Deterministic invalid replay payload, distinguishable from transient database failures. */
public class InvalidTelemetryWriteException extends IllegalArgumentException {
    public InvalidTelemetryWriteException(String message) { super(message); }
}
