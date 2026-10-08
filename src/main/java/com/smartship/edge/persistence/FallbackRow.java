package com.smartship.edge.persistence;

/** Durable fallback row, including the authoritative replay id stored in its column. */
public record FallbackRow(long id, String stream, String mmsi, String replayId, String payload) {}
