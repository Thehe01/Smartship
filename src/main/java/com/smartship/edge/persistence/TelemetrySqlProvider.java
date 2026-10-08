package com.smartship.edge.persistence;

import java.util.List;
import java.util.Map;

/** Providers select fixed schema statements and bind every value with #{...}. */
public final class TelemetrySqlProvider {
    public static String insert(Map<String, Object> parameters) {
        TelemetryStream stream = TelemetryStream.fromKey((String) parameters.get("stream"));
        Object[] args = (Object[]) parameters.get("args");
        validate(stream, args);
        return bind(stream.insertSql(), "args");
    }

    public static String insertEngineBatch(Map<String, Object> parameters) {
        @SuppressWarnings("unchecked")
        List<TelemetryWrite> rows = (List<TelemetryWrite>) parameters.get("rows");
        if (rows == null || rows.isEmpty()) throw new IllegalArgumentException("Empty engine batch");
        String sql = TelemetryStream.ENGINE.insertSql();
        int valuesIndex = sql.indexOf("VALUES");
        String values = sql.substring(valuesIndex + "VALUES".length()).trim();
        StringBuilder batch = new StringBuilder(sql.substring(0, valuesIndex)).append("VALUES ");
        for (int i = 0; i < rows.size(); i++) {
            validate(TelemetryStream.ENGINE, rows.get(i).args());
            if (i > 0) batch.append(',');
            batch.append(bind(values, "rows[" + i + "].args"));
        }
        return batch.toString();
    }

    private static void validate(TelemetryStream stream, Object[] args) {
        if (args == null || args.length != stream.argumentCount()) {
            throw new InvalidTelemetryWriteException("Invalid argument count for " + stream);
        }
        Object replayId = args[args.length - 1];
        if (!(replayId instanceof String id) || id.isBlank()) {
            throw new InvalidTelemetryWriteException("Missing replay_id");
        }
    }

    private static String bind(String sql, String property) {
        StringBuilder result = new StringBuilder();
        int index = 0;
        for (char c : sql.toCharArray()) {
            if (c == '?') result.append("#{").append(property).append('[')
                    .append(index++).append("],jdbcType=NULL}");
            else result.append(c);
        }
        return result.toString();
    }

    public static String incremental(Map<String, Object> parameters) {
        return "SELECT * FROM " + table(parameters)
                + " WHERE id > #{lastId} ORDER BY id ASC LIMIT #{limit}";
    }
    public static String byId(Map<String, Object> parameters) {
        return "SELECT * FROM " + table(parameters) + " WHERE id = #{id}";
    }
    public static String maxId(Map<String, Object> parameters) {
        return "SELECT COALESCE(MAX(id), 0) FROM " + table(parameters);
    }
    private static String table(Map<String, Object> parameters) {
        return TelemetryStream.requireTable((String) parameters.get("table"));
    }
}
