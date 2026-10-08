package com.smartship.edge.persistence;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** MyBatis access with explicit short transactions for batch and fallback replay. */
@Repository
public class EdgeTelemetryRepository {
    private final EdgeTelemetryMapper mapper;
    private final TransactionTemplate transaction;

    public EdgeTelemetryRepository(EdgeTelemetryMapper mapper, PlatformTransactionManager manager) {
        this.mapper = mapper;
        this.transaction = new TransactionTemplate(manager);
        this.transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public int insert(String stream, Object[] args) { return mapper.insert(stream, args); }
    public int insertFallback(Object[] args) { return mapper.insertFallback(args); }
    public List<FallbackRow> pendingFallback(long cursor, int limit) {
        return mapper.pendingFallback(cursor, limit);
    }

    public int insertEngineBatch(List<Object[]> rows) {
        if (rows == null || rows.isEmpty()) return 0;
        return transaction.execute(status -> {
            int inserted = mapper.insertEngineBatch(rows.stream().map(TelemetryWrite::new).toList());
            if (inserted != rows.size()) throw new IllegalStateException("Incomplete engine batch");
            return inserted;
        });
    }

    public void replayFallback(long id, String stream, Object[] args) {
        transaction.executeWithoutResult(status -> {
            try {
                mapper.insert(stream, args);
            } catch (DuplicateKeyException alreadyStored) {
                // MySQL/H2 duplicate statement does not abort this transaction.
            }
            mapper.deleteFallback(id);
        });
    }

    public List<Map<String, Object>> incremental(String table, long lastId, int limit) {
        return mapper.incremental(table, lastId, limit).stream().map(this::normalizeRow).toList();
    }
    public Map<String, Object> byId(String table, long id) {
        Map<String, Object> row = mapper.byId(table, id);
        return row == null ? null : normalizeRow(row);
    }
    public long maxId(String table) { return mapper.maxId(table); }
    public long cursor(String table) {
        Long id = mapper.cursor(table);
        return id == null ? 0L : id;
    }
    public long getOrCreateCursor(String table) {
        TelemetryStream.requireTable(table);
        Long id = mapper.cursor(table);
        if (id != null) return id;
        try {
            mapper.createCursor(table);
            return 0L;
        } catch (DuplicateKeyException raced) {
            return cursor(table);
        }
    }
    public void updateCursor(String table, long lastId) { mapper.updateCursor(table, lastId); }
    public void ensureCursorTable() { mapper.ensureCursorTable(); }

    private Map<String, Object> normalizeRow(Map<String, Object> row) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        row.forEach((key, value) -> normalized.put(key.toLowerCase(Locale.ROOT),
                value instanceof Timestamp ts ? ts.toLocalDateTime() : value));
        return normalized;
    }
}
