package com.smartship.edge.benchmark;

import com.smartship.edge.benchmark.support.BenchmarkFixtures;
import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PoolSnapshot;
import com.smartship.edge.routing.ShipDataSourceManager;
import com.smartship.edge.routing.ShipDataSourceContext;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;

/**
 * P1-4 多船动态 Hikari 路由 Benchmark（integration benchmark，非 JMH）。
 * <p>
 * 每船独立 H2 隔离库，测量首访/缓存/刷新三阶段延迟与连接池快照。
 * 本环境数字仅描述当前测试条件，不得直接宣称为生产环境性能。
 */
@Tag("benchmark")
@DisplayName("P1-4 Benchmark: multi-ship routing")
class MultiShipRoutingBenchmarkTest {

    private static final BenchmarkRecorder RECORDER = new BenchmarkRecorder(params());

    private static Map<String, String> params() {
        return Map.of(
                "benchmark.ships", prop("benchmark.ships", "1,10,50"),
                "benchmark.threads", prop("benchmark.threads", "32"));
    }

    private static String prop(String key, String def) {
        return System.getProperty(key, def);
    }

    @AfterAll
    static void flushReport() {
        RECORDER.flush();
    }

    private static List<Integer> intList(String key, String def) {
        List<Integer> out = new ArrayList<>();
        for (String part : prop(key, def).split(",")) {
            out.add(Integer.parseInt(part.trim()));
        }
        return out;
    }

    private static String mmsiOf(int i) {
        return "413" + String.format("%06d", 100 + i);
    }

    private static String shipDbUrl(String mmsi, String suffix) {
        return "jdbc:h2:mem:ship_" + mmsi + "_" + suffix + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
    }

    @Test
    @DisplayName("multiship-routing: 1/10/50 船首访-缓存-刷新全阶段")
    void multishipRouting() throws Exception {
        for (int ships : intList("benchmark.ships", "1,10,50")) {
            runShips(ships);
        }
    }

    private void runShips(int ships) throws Exception {
        final long validationIntervalMs = 3_600_000L;
        EdgeProperties properties = new EdgeProperties();
        properties.getDatasource().getShip().setDriverClassName("org.h2.Driver");
        properties.getDatasource().getShip().setMaximumPoolSize(5);
        properties.getDatasource().getShip().setRegistryValidationIntervalMs(validationIntervalMs);

        JdbcTemplate authReal = BenchmarkFixtures.newH2("bench_auth_" + ships + "_" + System.nanoTime());
        BenchmarkFixtures.createRegistryTable(authReal);
        for (int i = 0; i < ships; i++) {
            String mmsi = mmsiOf(i);
            BenchmarkFixtures.insertRegistryRow(authReal, "ship-" + i, mmsi,
                    "db_" + mmsi, shipDbUrl(mmsi, "v1"), true);
        }
        JdbcTemplate authSpy = spy(authReal);
        ShipDataSourceManager manager = new ShipDataSourceManager(authSpy, properties);
        Instant base = Instant.now();
        manager.setClock(Clock.fixed(base, ZoneOffset.UTC));
        try {
            BenchmarkRecorder.Scenario s = RECORDER.scenario("multiship-routing-n" + ships)
                    .param("ships", ships)
                    .param("same_mmsi_burst_threads", 50)
                    .param("fanout_threads", prop("benchmark.threads", "32"))
                    .param("registry_ttl_ms", validationIntervalMs)
                    .note("首访 50 线程同 MMSI 必须收敛为 1 Context / 1 池；缓存阶段 registry 查询必须为 0");

            // ---- 阶段一：同 MMSI 50 并发首访 ----
            String mmsi0 = mmsiOf(0);
            BurstResult first = burst(manager, 50, idx -> manager.getJdbcTemplate(null, mmsi0));
            List<Long> firstLat = first.latencies();
            assertEquals(1, manager.listPoolSnapshots().size(), "同 MMSI 首访必须只创建 1 个池");
            assertNotNull(manager.getDataSourceContext(mmsi0));
            for (long nanos : firstLat) {
                s.recordLatencyNanos(nanos);
            }
            s.count("first_access_calls", firstLat.size())
                    .count("contexts_after_first", manager.listPoolSnapshots().size());
            s.observePeak("first_p95_ms", toMillis(p95(firstLat)));

            // ---- 阶段二：N 船扇出并发（每船恰好访问一次，保证 N 个池全被创建） ----
            int fanoutThreads = Integer.parseInt(prop("benchmark.threads", "32"));
            BurstResult fanout = burstFanout(manager, fanoutThreads, ships);
            List<Long> fanoutLat = fanout.latencies();
            assertEquals(ships, manager.listPoolSnapshots().size(), "N 船必须恰好 N 个池");
            for (long nanos : fanoutLat) {
                s.recordLatencyNanos(nanos);
            }
            s.count("fanout_calls", fanoutLat.size())
                    .count("pools_created", manager.listPoolSnapshots().size());
            s.observePeak("fanout_p95_ms", toMillis(p95(fanoutLat)));
            long totalActive = 0;
            long totalIdle = 0;
            for (PoolSnapshot snap : manager.listPoolSnapshots()) {
                totalActive += snap.getActiveConnections();
                totalIdle += snap.getIdleConnections();
            }
            s.observePeak("connections_active", totalActive)
                    .observePeak("connections_idle", totalIdle);

            // ---- 阶段三：缓存访问零 registry 查询 ----
            Mockito.clearInvocations(authSpy);
            BurstResult cached = burst(manager, fanoutThreads,
                    idx -> manager.getJdbcTemplate(null, mmsiOf(idx % ships)));
            List<Long> cachedLat = cached.latencies();
            long registryQueries = Mockito.mockingDetails(authSpy).getInvocations().size();
            for (long nanos : cachedLat) {
                s.recordLatencyNanos(nanos);
            }
            s.count("cached_calls", cachedLat.size())
                    .count("cached_registry_queries", registryQueries);
            s.observePeak("cached_p95_ms", toMillis(p95(cachedLat)));
            assertEquals(0, registryQueries, "TTL 未过期缓存访问不得查询 registry");

            // ---- 阶段四：配置变更 + TTL 跳跃触发刷新 ----
            ShipDataSourceContext before = manager.getDataSourceContext(mmsi0);
            HikariDataSource oldDs = before.getDataSource();
            authReal.update("UPDATE ship_database_registry SET host = ? WHERE mmsi = ?",
                    shipDbUrl(mmsi0, "v2"), mmsi0);
            manager.setClock(Clock.fixed(base.plusMillis(validationIntervalMs * 2), ZoneOffset.UTC));
            long r0 = System.nanoTime();
            JdbcTemplate refreshed = manager.getJdbcTemplate(null, mmsi0);
            long refreshMs = (System.nanoTime() - r0) / 1_000_000L;
            assertEquals(1, refreshed.queryForObject("SELECT 1", Integer.class));
            assertTrue(oldDs.isClosed(), "刷新后旧池必须关闭");
            assertEquals(ships, manager.listPoolSnapshots().size(), "刷新不得泄漏连接池");
            s.observePeak("refresh_ms", refreshMs)
                    .count("pools_after_refresh", manager.listPoolSnapshots().size());

            s.addRunThroughput((firstLat.size() + fanoutLat.size() + cachedLat.size())
                    / ((first.wallNanos() + fanout.wallNanos() + cached.wallNanos()) / 1e9));
            RECORDER.complete(s);
        } finally {
            manager.closeAll();
        }
    }

    private interface Access {
        JdbcTemplate get(int idx);
    }

    private record BurstResult(List<Long> latencies, long wallNanos) {
    }

    /**
     * 扇出突发：N 艘船每船恰好访问一次（任务数 = 船数，线程数为配置值），
     * 保证全部 N 个连接池都被创建，避免任务数 &lt; 船数时部分船只未被触及。
     */
    private static BurstResult burstFanout(ShipDataSourceManager manager, int threads, int ships)
            throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(ships);
            List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>(ships));
            List<Throwable> errors = java.util.Collections.synchronizedList(new ArrayList<>());
            for (int i = 0; i < ships; i++) {
                final String mmsi = mmsiOf(i);
                pool.submit(() -> {
                    try {
                        start.await();
                        long t0 = System.nanoTime();
                        JdbcTemplate jt = manager.getJdbcTemplate(null, mmsi);
                        jt.execute("SELECT 1");
                        latencies.add(System.nanoTime() - t0);
                    } catch (Throwable t) {
                        errors.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            long wall0 = System.nanoTime();
            start.countDown();
            assertTrue(done.await(120, TimeUnit.SECONDS), "扇出突发必须在 120s 内完成");
            long wallNanos = System.nanoTime() - wall0;
            assertTrue(errors.isEmpty(), "并发访问不得异常: " + errors);
            return new BurstResult(latencies, wallNanos);
        } finally {
            pool.shutdownNow();
        }
    }

    private static BurstResult burst(ShipDataSourceManager manager, int threads, Access access)
            throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>(threads));
            List<Throwable> errors = java.util.Collections.synchronizedList(new ArrayList<>());
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        start.await();
                        long t0 = System.nanoTime();
                        JdbcTemplate jt = access.get(idx);
                        jt.execute("SELECT 1");
                        latencies.add(System.nanoTime() - t0);
                    } catch (Throwable t) {
                        errors.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            long wall0 = System.nanoTime();
            start.countDown();
            assertTrue(done.await(60, TimeUnit.SECONDS), "并发突发必须在 60s 内完成");
            long wallNanos = System.nanoTime() - wall0;
            assertTrue(errors.isEmpty(), "并发访问不得异常: " + errors);
            return new BurstResult(latencies, wallNanos);
        } finally {
            pool.shutdownNow();
        }
    }

    private static long p95(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compare);
        return BenchmarkRecorder.percentile(sorted, 95);
    }

    private static double toMillis(long nanos) {
        return nanos / 1_000_000.0;
    }
}
