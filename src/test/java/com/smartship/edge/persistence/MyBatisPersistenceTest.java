package com.smartship.edge.persistence;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.service.NmeaDataPersistenceService;
import java.time.LocalDateTime;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

class MyBatisPersistenceTest {
    private JdbcTemplate jdbc;
    private EdgeTelemetryMapper mapper;
    private EdgeTelemetryRepository repository;

    @BeforeEach
    void setup() throws Exception {
        var ds = new DriverManagerDataSource("jdbc:h2:mem:mybatis_" + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "");
        String schema = new ClassPathResource("schema/ship-schema.sql")
                .getContentAsString(StandardCharsets.UTF_8);
        // H2 scopes secondary index names per schema; MySQL scopes them per table.
        // Keep all columns and unique constraints, omit only the secondary indexes.
        schema = schema.replaceAll("(?m)^\\s*INDEX \\w+ \\([^\\r\\n]+\\),\\s*$", "");
        new ResourceDatabasePopulator(new ByteArrayResource(schema.getBytes(StandardCharsets.UTF_8))).execute(ds);
        jdbc = new JdbcTemplate(ds);
        var factory = new SqlSessionFactoryBean();
        factory.setDataSource(ds);
        var configuration = new org.apache.ibatis.session.Configuration();
        configuration.setCallSettersOnNulls(true);
        configuration.addMapper(EdgeTelemetryMapper.class);
        factory.setConfiguration(configuration);
        mapper = spy(new SqlSessionTemplate(factory.getObject()).getMapper(EdgeTelemetryMapper.class));
        repository = new EdgeTelemetryRepository(mapper, new DataSourceTransactionManager(ds));
    }

    @Test
    void fiveStreamsUseRealMapperAndKeepNullableUploadFields() {
        var properties = new EdgeProperties();
        properties.setMmsi("413999999");
        properties.setShipId("S001");
        properties.setSchemaReady(true);
        properties.getCollect().getPersist().setEnabled(true);
        var service = new NmeaDataPersistenceService(repository, properties, null);
        service.saveGps("RMC", "serial", 31.2, 121.5, null, null, null, null,
                null, null, null, null, null, null, "413999999");
        service.saveWind("MWV", "serial", 10.0, 5.0, null, null, null, "413999999");
        service.saveDepth("DPT", "serial", 12.5, null, "413999999");
        service.saveRudder("RSA", "serial", 3.5, "413999999");
        service.saveEngine("413999999", 1, 1500, 85, .5, .4, 420, .25,
                2.8, 75, 24.5, 12000, 1, 0, 0);
        for (String stream : List.of("gps", "wind", "depth", "rudder", "engine")) {
            var row = repository.incremental("zncb_" + stream + "_data", 0, 10).get(0);
            assertEquals("S001", row.get("ship_id"));
            assertEquals("413999999", row.get("mmsi"));
            assertInstanceOf(LocalDateTime.class, row.get("timestamp"));
            assertTrue(row.get("replay_id") instanceof String id && !id.isBlank());
            assertEquals(1L, jdbc.queryForObject("SELECT COUNT(*) FROM zncb_" + stream + "_data", Long.class));
        }
        var gps = repository.incremental("zncb_gps_data", 0, 10).get(0);
        assertTrue(gps.containsKey("speed_knots"));
        assertNull(gps.get("speed_knots"));
        assertEquals(31.2, ((Number) gps.get("latitude")).doubleValue(), .0000001);
    }

    @Test
    void failedBatchRollsBackBeforeRetryAndDuplicateRemainsSpringException() {
        doAnswer(invocation -> {
            invocation.callRealMethod();
            return 0; // force the repository's completeness check after real SQL execution
        }).when(mapper).insertEngineBatch(anyList());
        Object[] args = engineArgs("batch-1");
        assertThrows(IllegalStateException.class, () -> repository.insertEngineBatch(List.<Object[]>of(args)));
        assertEquals(0L, jdbc.queryForObject("SELECT COUNT(*) FROM zncb_engine_data", Long.class));
        assertEquals(1, repository.insert("engine", args));
        assertThrows(DuplicateKeyException.class, () -> repository.insert("engine", args));
        assertEquals(1L, jdbc.queryForObject("SELECT COUNT(*) FROM zncb_engine_data", Long.class));
        doCallRealMethod().when(mapper).insertEngineBatch(anyList());
        assertEquals(2, repository.insertEngineBatch(List.of(engineArgs("batch-2"), engineArgs("batch-3"))));
        assertEquals(3L, jdbc.queryForObject("SELECT COUNT(*) FROM zncb_engine_data", Long.class));
    }

    @Test
    void replayInsertAndDeleteRollBackTogether() {
        repository.insertFallback(new Object[]{"engine", "413999999", "replay-1", "{}", "down"});
        long id = repository.pendingFallback(0, 10).get(0).id();
        doThrow(new RuntimeException("delete failed")).when(mapper).deleteFallback(id);
        assertThrows(RuntimeException.class, () -> repository.replayFallback(id, "engine", engineArgs("replay-1")));
        assertEquals(0L, jdbc.queryForObject("SELECT COUNT(*) FROM zncb_engine_data", Long.class));
        assertEquals(1, repository.pendingFallback(0, 10).size());
        doCallRealMethod().when(mapper).deleteFallback(id);
        repository.replayFallback(id, "engine", engineArgs("replay-1"));
        assertEquals(1L, jdbc.queryForObject("SELECT COUNT(*) FROM zncb_engine_data", Long.class));
        assertTrue(repository.pendingFallback(0, 10).isEmpty());
    }

    @Test
    void identifiersAndMalformedReplayArgumentsAreRejectedBeforeSql() {
        assertThrows(Exception.class, () -> repository.incremental("zncb_gps_data; DROP TABLE zncb_gps_data", 0, 10));
        assertThrows(Exception.class, () -> repository.insert("engine", new Object[]{"short"}));
        var args = engineArgs("");
        assertThrows(Exception.class, () -> repository.insert("engine", args));
        assertEquals(0L, jdbc.queryForObject("SELECT COUNT(*) FROM zncb_engine_data", Long.class));
    }

    private Object[] engineArgs(String replayId) {
        return new Object[]{"S001", "413999999", 1, LocalDateTime.of(2026, 10, 9, 0, 0),
                1500d, 85d, .5d, .4d, 420d, .25d, 2.8d, 75d, 24.5d, 12000, 1, 0, 0, replayId};
    }
}
