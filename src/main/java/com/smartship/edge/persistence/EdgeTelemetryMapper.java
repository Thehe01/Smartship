package com.smartship.edge.persistence;

import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.*;

@Mapper
public interface EdgeTelemetryMapper {
    @InsertProvider(type = TelemetrySqlProvider.class, method = "insert")
    int insert(@Param("stream") String stream, @Param("args") Object[] args);

    @InsertProvider(type = TelemetrySqlProvider.class, method = "insertEngineBatch")
    int insertEngineBatch(@Param("rows") List<TelemetryWrite> rows);

    @Insert("""
        INSERT INTO zncb_failed_writes (stream, mmsi, replay_id, payload, error)
        VALUES (#{args[0],jdbcType=VARCHAR}, #{args[1],jdbcType=VARCHAR},
                #{args[2],jdbcType=VARCHAR}, #{args[3],jdbcType=LONGVARCHAR},
                #{args[4],jdbcType=VARCHAR})
        """)
    int insertFallback(@Param("args") Object[] args);

    @Select("""
        SELECT id, stream, mmsi, replay_id, payload FROM zncb_failed_writes
        WHERE id > #{cursor} ORDER BY id ASC LIMIT #{limit}
        """)
    @ConstructorArgs({
        @Arg(column = "id", javaType = long.class),
        @Arg(column = "stream", javaType = String.class),
        @Arg(column = "mmsi", javaType = String.class),
        @Arg(column = "replay_id", javaType = String.class),
        @Arg(column = "payload", javaType = String.class)
    })
    List<FallbackRow> pendingFallback(@Param("cursor") long cursor, @Param("limit") int limit);

    @Delete("DELETE FROM zncb_failed_writes WHERE id = #{id}")
    int deleteFallback(@Param("id") long id);

    @SelectProvider(type = TelemetrySqlProvider.class, method = "incremental")
    List<Map<String, Object>> incremental(@Param("table") String table,
            @Param("lastId") long lastId, @Param("limit") int limit);

    @SelectProvider(type = TelemetrySqlProvider.class, method = "byId")
    Map<String, Object> byId(@Param("table") String table, @Param("id") long id);

    @SelectProvider(type = TelemetrySqlProvider.class, method = "maxId")
    long maxId(@Param("table") String table);

    @Select("SELECT last_uploaded_id FROM zncb_upload_cursor WHERE stream_name = #{table} AND partition_key = ''")
    Long cursor(@Param("table") String table);

    @Insert("""
        INSERT INTO zncb_upload_cursor (stream_name, partition_key, last_uploaded_id, updated_at)
        VALUES (#{table}, '', 0, NOW())
        """)
    int createCursor(@Param("table") String table);

    @Update("""
        UPDATE zncb_upload_cursor SET last_uploaded_id = #{lastId}, updated_at = NOW()
        WHERE stream_name = #{table} AND partition_key = ''
        """)
    int updateCursor(@Param("table") String table, @Param("lastId") long lastId);

    @Update("""
        CREATE TABLE IF NOT EXISTS zncb_upload_cursor (
            stream_name VARCHAR(64) NOT NULL,
            partition_key VARCHAR(64) NOT NULL DEFAULT '',
            last_uploaded_id BIGINT NOT NULL DEFAULT 0,
            last_uploaded_time DATETIME NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (stream_name, partition_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
    void ensureCursorTable();
}
