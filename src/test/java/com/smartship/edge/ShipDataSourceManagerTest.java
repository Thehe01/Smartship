package com.smartship.edge;

import com.smartship.edge.config.EdgeProperties;
import com.smartship.edge.routing.PoolSnapshot;
import com.smartship.edge.routing.ShipDataSourceContext;
import com.smartship.edge.routing.ShipDataSourceManager;
import com.smartship.edge.routing.exception.RegistryQueryException;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P1-2: Dynamic HikariCP Lifecycle Governance 核心治理机制全场景测试套件
 */
class ShipDataSourceManagerTest {

    private DriverManagerDataSource authDataSource;
    private JdbcTemplate authJdbcTemplate;
    private EdgeProperties properties;
    private ShipDataSourceManager manager;

    @BeforeEach
    void setUp() {
        authDataSource = new DriverManagerDataSource("jdbc:h2:mem:auth_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "");
        authJdbcTemplate = new JdbcTemplate(authDataSource);

        authJdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS ship_database_registry (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    ship_id VARCHAR(64),
                    mmsi VARCHAR(32) NOT NULL UNIQUE,
                    database_name VARCHAR(64) NOT NULL,
                    host VARCHAR(255) DEFAULT 'localhost',
                    port INT DEFAULT 3306,
                    username VARCHAR(64) DEFAULT 'root',
                    password VARCHAR(128) DEFAULT '123456',
                    enabled TINYINT(1) DEFAULT 1
                )
                """);

        properties = new EdgeProperties();
        properties.getDatasource().getShip().setDriverClassName("org.h2.Driver");
        properties.getDatasource().getShip().setMaximumPoolSize(5);

        manager = new ShipDataSourceManager(authJdbcTemplate, properties);
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.closeAll();
        }
    }

    private void insertShip(String shipId, String mmsi, String dbName, String host, int port,
                            String user, String pass, boolean enabled) {
        authJdbcTemplate.update("""
                INSERT INTO ship_database_registry (ship_id, mmsi, database_name, host, port, username, password, enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, shipId, mmsi, dbName, host, port, user, pass, enabled ? 1 : 0);
    }

    @Test
    @DisplayName("测试场景 1: same MMSI reuses one datasource (同一 MMSI 复用同一个数据源连接池与 JdbcTemplate)")
    void testSameMmsiReusesOneDatasource() {
        String mmsi = "413999999";
        String h2Url = "jdbc:h2:mem:ship_db_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url, 0, "sa", "", true);

        JdbcTemplate jt1 = manager.getJdbcTemplate(null, mmsi);
        JdbcTemplate jt2 = manager.getJdbcTemplate(null, mmsi);
        JdbcTemplate jt3 = manager.getJdbcTemplate("ship-1", null);
        JdbcTemplate jt4 = manager.getJdbcTemplate(mmsi);

        assertNotNull(jt1);
        assertSame(jt1, jt2, "多次通过 MMSI 获取必须返回同一 JdbcTemplate 实例");
        assertSame(jt1, jt3, "通过 shipId 路由必须命中同一 MMSI 的 JdbcTemplate 实例");
        assertSame(jt1, jt4, "便捷方法 getJdbcTemplate(mmsi) 必须返回同一实例");

        ShipDataSourceContext context = manager.getDataSourceContext(mmsi);
        assertNotNull(context);
        assertEquals(mmsi, context.getRegistry().mmsi());
        assertFalse(context.isClosed());
        assertEquals(1, manager.listPoolSnapshots().size(), "全局有且仅有 1 个活动的 Hikari 连接池");
    }

    @Test
    @DisplayName("测试场景 2: unchanged fingerprint reuses existing pool (指纹未变化时刷新操作安全复用现有连接池)")
    void testUnchangedFingerprintReusesExistingPool() {
        String mmsi = "413999999";
        String h2Url = "jdbc:h2:mem:ship_db_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url, 0, "sa", "", true);

        JdbcTemplate jtBefore = manager.getJdbcTemplate(null, mmsi);
        ShipDataSourceContext ctxBefore = manager.getDataSourceContext(mmsi);
        HikariDataSource dsBefore = ctxBefore.getDataSource();
        String fingerprintBefore = ctxBefore.getConfigFingerprint();

        // 执行刷新，由于配置未变化，返回 false
        boolean refreshed = manager.refreshDataSource(mmsi);
        assertFalse(refreshed, "配置未改变时，refreshDataSource 应返回 false 表示复用");

        ShipDataSourceContext ctxAfter = manager.getDataSourceContext(mmsi);
        HikariDataSource dsAfter = ctxAfter.getDataSource();

        assertSame(ctxBefore, ctxAfter, "指纹未变时必须保留原 Context");
        assertSame(dsBefore, dsAfter, "指纹未变时必须保留原 HikariDataSource");
        assertEquals(fingerprintBefore, ctxAfter.getConfigFingerprint(), "指纹必须保持恒定");
        assertFalse(dsAfter.isClosed(), "复用的连接池必须保持存活状态");

        JdbcTemplate jtAfter = manager.getJdbcTemplate(null, mmsi);
        assertSame(jtBefore, jtAfter);
    }

    @Test
    @DisplayName("测试场景 3: changed config refreshes pool (配置变化时指纹改变，触发新建池并替换旧池)")
    void testChangedConfigRefreshesPool() {
        String mmsi = "413999999";
        String h2Url1 = "jdbc:h2:mem:ship_db_v1_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url1, 0, "sa", "", true);

        JdbcTemplate jt1 = manager.getJdbcTemplate(null, mmsi);
        ShipDataSourceContext ctx1 = manager.getDataSourceContext(mmsi);
        HikariDataSource ds1 = ctx1.getDataSource();
        String fp1 = ctx1.getConfigFingerprint();

        // 修改元数据表中的配置 (修改 host URL)
        String h2Url2 = "jdbc:h2:mem:ship_db_v2_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        authJdbcTemplate.update("UPDATE ship_database_registry SET host = ? WHERE mmsi = ?", h2Url2, mmsi);

        // 触发刷新
        boolean refreshed = manager.refreshDataSource(mmsi);
        assertTrue(refreshed, "配置改变时，refreshDataSource 应返回 true");

        ShipDataSourceContext ctx2 = manager.getDataSourceContext(mmsi);
        HikariDataSource ds2 = ctx2.getDataSource();
        String fp2 = ctx2.getConfigFingerprint();

        assertNotSame(ctx1, ctx2, "必须生成全新的上下文对象");
        assertNotSame(ds1, ds2, "必须生成全新的 HikariDataSource");
        assertNotEquals(fp1, fp2, "指纹必须随着配置变化而更新");
        assertTrue(ds1.isClosed(), "旧连接池必须已被关闭");
        assertFalse(ds2.isClosed(), "新连接池必须处于活跃状态");

        // 验证新 JdbcTemplate 能够正常执行查询
        JdbcTemplate jt2 = manager.getJdbcTemplate(null, mmsi);
        assertNotSame(jt1, jt2);
        Integer result = jt2.queryForObject("SELECT 1", Integer.class);
        assertEquals(1, result);
    }

    @Test
    @DisplayName("测试场景 4: old pool closes only after new pool succeeds (遵循 create new -> replace context -> close old 顺序)")
    void testOldPoolClosesOnlyAfterNewPoolSucceeds() {
        String mmsi = "413999999";
        String h2Url1 = "jdbc:h2:mem:ship_order_v1_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url1, 0, "sa", "", true);

        AtomicReference<HikariDataSource> probeOldDs = new AtomicReference<>();
        AtomicBoolean oldPoolWasOpenDuringNewPoolCreation = new AtomicBoolean(false);

        // 继承管理器并重写 createDataSource 探针，检查创建新池时旧池的状态
        ShipDataSourceManager probeManager = new ShipDataSourceManager(authJdbcTemplate, properties) {
            @Override
            protected HikariDataSource createDataSource(ShipDatabase registry) {
                HikariDataSource old = probeOldDs.get();
                // 在新池创建期间探测旧池是否依旧开放
                if (old != null && !old.isClosed()) {
                    oldPoolWasOpenDuringNewPoolCreation.set(true);
                }
                return super.createDataSource(registry);
            }
        };

        // 1. 初始化旧连接池
        probeManager.getJdbcTemplate(null, mmsi);
        ShipDataSourceContext ctx1 = probeManager.getDataSourceContext(mmsi);
        HikariDataSource ds1 = ctx1.getDataSource();
        probeOldDs.set(ds1);
        assertFalse(ds1.isClosed());

        // 2. 更新配置
        String h2Url2 = "jdbc:h2:mem:ship_order_v2_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        authJdbcTemplate.update("UPDATE ship_database_registry SET host = ? WHERE mmsi = ?", h2Url2, mmsi);

        // 3. 触发刷新
        boolean refreshed = probeManager.refreshDataSource(mmsi);
        assertTrue(refreshed);

        // 4. 验证生命周期拓扑序：创建新池时，旧池决不能被提前关闭
        assertTrue(oldPoolWasOpenDuringNewPoolCreation.get(), "创建新连接池期间，旧连接池必须依然保持存活开放状态！");

        // 5. 刷新完成后旧池才关闭
        assertTrue(ds1.isClosed(), "新连接池就绪且上下文替换完成后，旧连接池才被安全关闭");
        assertFalse(probeManager.getDataSourceContext(mmsi).getDataSource().isClosed());
        probeManager.closeAll();
    }

    @Test
    @DisplayName("测试场景 5: failed refresh keeps old pool intact (新连接池创建失败时，旧池不受任何破坏继续提供服务)")
    void testFailedRefreshKeepsOldPoolIntact() {
        String mmsi = "413999999";
        String h2Url1 = "jdbc:h2:mem:ship_fail_test_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url1, 0, "sa", "", true);

        JdbcTemplate jt1 = manager.getJdbcTemplate(null, mmsi);
        // 在旧库中建表并插入数据
        jt1.execute("CREATE TABLE test_data (id INT)");
        jt1.execute("INSERT INTO test_data VALUES (888)");

        ShipDataSourceContext ctxBefore = manager.getDataSourceContext(mmsi);
        HikariDataSource dsBefore = ctxBefore.getDataSource();
        assertFalse(dsBefore.isClosed());

        // 更新为一个非法的 JDBC URL，导致 HikariPool 初始化必然抛出异常
        String invalidUrl = "jdbc:invalid_protocol://127.0.0.1:99999/bad_db";
        authJdbcTemplate.update("UPDATE ship_database_registry SET host = ? WHERE mmsi = ?", invalidUrl, mmsi);

        // 触发刷新，必须抛出异常
        assertThrows(Exception.class, () -> manager.refreshDataSource(mmsi),
                "非法配置创建连接池失败时必须抛出异常，通知治理层");

        // 验证上下文没有被清空或替换为损坏对象
        ShipDataSourceContext ctxAfter = manager.getDataSourceContext(mmsi);
        HikariDataSource dsAfter = ctxAfter.getDataSource();

        assertSame(ctxBefore, ctxAfter, "刷新失败时上下文必须保持不变");
        assertSame(dsBefore, dsAfter, "刷新失败时必须保留原有有效 HikariDataSource 实例");
        assertFalse(dsAfter.isClosed(), "原连接池绝不能被提前关闭");

        // 原连接池必须依然可以正常执行查询
        JdbcTemplate jtAfter = manager.getJdbcTemplate(null, mmsi);
        assertSame(jt1, jtAfter);
        Integer val = jtAfter.queryForObject("SELECT id FROM test_data LIMIT 1", Integer.class);
        assertEquals(888, val, "原连接池数据读写功能不受损");
    }

    @Test
    @DisplayName("测试场景 6: disabled ship evicts and closes pool (禁用船舶主动驱逐并关闭已有连接池，后续拒绝访问)")
    void testDisabledShipEvictsAndClosesPool() {
        String mmsi = "413999999";
        String h2Url = "jdbc:h2:mem:ship_disabled_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url, 0, "sa", "", true);

        // 初始正常访问
        manager.getJdbcTemplate(null, mmsi);
        ShipDataSourceContext context = manager.getDataSourceContext(mmsi);
        HikariDataSource ds = context.getDataSource();
        assertFalse(ds.isClosed());

        // 将船舶在注册表中标记为禁用
        authJdbcTemplate.update("UPDATE ship_database_registry SET enabled = 0 WHERE mmsi = ?", mmsi);

        // 调用 refreshDataSource 检测到禁用，执行驱逐
        boolean refreshed = manager.refreshDataSource(mmsi);
        assertFalse(refreshed);

        // 验证连接池被驱逐并关闭
        assertNull(manager.getDataSourceContext(mmsi), "禁用船舶必须从缓存中被驱逐");
        assertNull(manager.getPoolSnapshot(mmsi), "禁用船舶指标快照必须返回 null");
        assertTrue(ds.isClosed(), "被驱逐船舶的底层连接池必须被彻底关闭");

        // 后续调用 getJdbcTemplate 坚决拦截抛出 IllegalStateException
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> manager.getJdbcTemplate(null, mmsi));
        assertTrue(ex.getMessage().contains("未在注册表中配置或已被禁用"));
    }

    @Test
    @DisplayName("测试场景 7: invalidate is idempotent (显式失效操作必须幂等，重复调用安全无副作用)")
    void testInvalidateIsIdempotent() {
        String mmsi = "413999999";
        String h2Url = "jdbc:h2:mem:ship_inv_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url, 0, "sa", "", true);

        manager.getJdbcTemplate(null, mmsi);
        HikariDataSource ds = manager.getDataSourceContext(mmsi).getDataSource();
        assertFalse(ds.isClosed());

        // 第一次失效：成功驱逐并关闭，返回 true
        boolean firstInv = manager.invalidateDataSource(mmsi);
        assertTrue(firstInv);
        assertTrue(ds.isClosed());
        assertNull(manager.getDataSourceContext(mmsi));

        // 第二次失效（幂等）：不再有缓存，返回 false，不抛任何异常
        boolean secondInv = manager.invalidateDataSource(mmsi);
        assertFalse(secondInv);

        // 对不存在的 MMSI 调用失效：返回 false，不抛任何异常
        boolean nonExistentInv = manager.invalidateDataSource("non_existent_mmsi");
        assertFalse(nonExistentInv);
    }

    @Test
    @DisplayName("测试场景 8: closeAll closes every datasource (应用停机统一关闭全部连接池且具备幂等性)")
    void testCloseAllClosesEveryDatasource() {
        String mmsi1 = "413000001";
        String mmsi2 = "413000002";
        String h2Url1 = "jdbc:h2:mem:ship_all_1;DB_CLOSE_DELAY=-1;MODE=MySQL";
        String h2Url2 = "jdbc:h2:mem:ship_all_2;DB_CLOSE_DELAY=-1;MODE=MySQL";

        insertShip("ship-1", mmsi1, "zncb_ship_1", h2Url1, 0, "sa", "", true);
        insertShip("ship-2", mmsi2, "zncb_ship_2", h2Url2, 0, "sa", "", true);

        manager.getJdbcTemplate(null, mmsi1);
        manager.getJdbcTemplate(null, mmsi2);

        HikariDataSource ds1 = manager.getDataSourceContext(mmsi1).getDataSource();
        HikariDataSource ds2 = manager.getDataSourceContext(mmsi2).getDataSource();

        assertEquals(2, manager.listPoolSnapshots().size());
        assertFalse(ds1.isClosed());
        assertFalse(ds2.isClosed());

        // 执行统一关闭
        manager.closeAll();

        assertTrue(ds1.isClosed(), "ds1 必须已被关闭");
        assertTrue(ds2.isClosed(), "ds2 必须已被关闭");
        assertTrue(manager.listPoolSnapshots().isEmpty(), "快照列表必须为空");
        assertNull(manager.getDataSourceContext(mmsi1));
        assertNull(manager.getDataSourceContext(mmsi2));

        // 再次调用 closeAll 验证幂等无异常
        assertDoesNotThrow(() -> manager.closeAll());
    }

    @Test
    @DisplayName("测试场景 9: pool snapshot exposes valid Hikari metrics (PoolSnapshot 暴露完整且准确的 Hikari 指标)")
    void testPoolSnapshotExposesValidHikariMetrics() throws Exception {
        String mmsi = "413999999";
        String h2Url = "jdbc:h2:mem:ship_metrics_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url, 0, "sa", "", true);

        manager.getJdbcTemplate(null, mmsi);

        PoolSnapshot snapshot = manager.getPoolSnapshot(mmsi);
        assertNotNull(snapshot, "快照对象不应为 null");

        // 验证暴露的核心指标字段（支持 record 访问器与 JavaBean getter）
        assertEquals("ship-db-" + mmsi, snapshot.poolName());
        assertEquals("ship-db-" + mmsi, snapshot.getPoolName());
        assertTrue(snapshot.totalConnections() >= 0);
        assertTrue(snapshot.getTotalConnections() >= 0);
        assertTrue(snapshot.activeConnections() >= 0);
        assertTrue(snapshot.getActiveConnections() >= 0);
        assertTrue(snapshot.idleConnections() >= 0);
        assertTrue(snapshot.getIdleConnections() >= 0);
        assertEquals(0, snapshot.pendingThreads());
        assertEquals(0, snapshot.getPendingThreads());
        assertFalse(snapshot.closed());
        assertFalse(snapshot.isClosed());
        assertTrue(snapshot.createdAt() > 0);
        assertTrue(snapshot.getCreatedAt() > 0);

        // toString 验证
        String str = snapshot.toString();
        assertNotNull(str);
        assertTrue(str.contains("ship-db-" + mmsi));
        assertTrue(str.contains("active="));

        // 深度验证：真实借出一条物理连接，验证 activeConnections 和 totalConnections 真实动态变更
        HikariDataSource ds = manager.getDataSourceContext(mmsi).getDataSource();
        try (Connection conn = ds.getConnection()) {
            PoolSnapshot inUseSnapshot = manager.getPoolSnapshot(mmsi);
            assertNotNull(inUseSnapshot);
            assertEquals(1, inUseSnapshot.activeConnections(), "借出连接时 activeConnections 应为 1");
            assertEquals(1, inUseSnapshot.getActiveConnections());
            assertTrue(inUseSnapshot.totalConnections() >= 1, "借出连接时 totalConnections 应至少为 1");
        }

        // 连接归还后，activeConnections 恢复为 0
        PoolSnapshot returnedSnapshot = manager.getPoolSnapshot(mmsi);
        assertEquals(0, returnedSnapshot.activeConnections(), "归还连接后 activeConnections 应恢复为 0");
        assertTrue(returnedSnapshot.idleConnections() >= 1, "连接归还后 idleConnections 应恢复");

        // 验证全量列表快照
        List<PoolSnapshot> list = manager.listPoolSnapshots();
        assertEquals(1, list.size());
        assertEquals("ship-db-" + mmsi, list.get(0).poolName());
    }

    @Test
    @DisplayName("测试场景 10: concurrent first access creates only one surviving pool (50 线程高并发首次访问，仅创建 1 个有效连接池)")
    void testConcurrentFirstAccessCreatesOnlyOneSurvivingPool() throws InterruptedException {
        String mmsi = "413999999";
        String h2Url = "jdbc:h2:mem:ship_concurrent_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url, 0, "sa", "", true);

        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);

        List<JdbcTemplate> results = Collections.synchronizedList(new ArrayList<>());
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // 所有线程同一起跑线
                    JdbcTemplate jt = manager.getJdbcTemplate(null, mmsi);
                    // 顺便验证执行查询能力
                    jt.execute("SELECT 1");
                    results.add(jt);
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // 统一起跑
        startLatch.countDown();
        assertTrue(finishLatch.await(10, TimeUnit.SECONDS), "所有 50 个线程应在 10 秒内并发执行完毕");
        executor.shutdown();

        assertTrue(errors.isEmpty(), "所有并发请求均应成功，不能出现异常: " + errors);
        assertEquals(threadCount, results.size());

        // 验证所有 50 个线程返回的 JdbcTemplate 实例完全同一
        JdbcTemplate firstJt = results.get(0);
        for (JdbcTemplate jt : results) {
            assertSame(firstJt, jt, "并发获取的 JdbcTemplate 必须完全指向同一个上下文");
        }

        // 核心断言：最终只保留 1 个活动 Context，只有 1 个有效 HikariPool，连接池绝无泄漏
        assertEquals(1, manager.listPoolSnapshots().size(), "最终只能保留 1 个活跃连接池");
        ShipDataSourceContext context = manager.getDataSourceContext(mmsi);
        assertNotNull(context);
        assertFalse(context.isClosed(), "保留的唯一连接池必须健康存活");
    }

    @Test
    @DisplayName("测试场景 11: registry query exception does not evict normal pool (主认证库查询异常时抛出 RegistryQueryException 且不误删正常连接池)")
    void testRegistryQueryExceptionDoesNotEvictNormalPool() {
        String mmsi = "413999999";
        String h2Url = "jdbc:h2:mem:ship_registry_err_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url, 0, "sa", "", true);

        // 正常建立连接池
        manager.getJdbcTemplate(null, mmsi);
        ShipDataSourceContext ctxBefore = manager.getDataSourceContext(mmsi);
        HikariDataSource dsBefore = ctxBefore.getDataSource();
        assertFalse(dsBefore.isClosed());

        // 破坏主认证注册表结构模拟主认证库异常（例如主库宕机或表损坏）
        authJdbcTemplate.execute("DROP TABLE ship_database_registry");

        // resolveRegistry 此时应当抛出 RegistryQueryException，而不是返回 null（避免误判为船舶不存在）
        assertThrows(RegistryQueryException.class, () -> manager.resolveRegistry(null, mmsi));

        // 尝试刷新时遇到主认证库异常，必须向上抛出异常，绝不能误删原本工作正常的连接池！
        assertThrows(RegistryQueryException.class, () -> manager.refreshDataSource(mmsi));

        // 验证原本的连接池依旧安然无恙
        ShipDataSourceContext ctxAfter = manager.getDataSourceContext(mmsi);
        assertSame(ctxBefore, ctxAfter, "主认证库查询异常时，绝不能误删原正常连接池");
        assertFalse(dsBefore.isClosed(), "原连接池绝不能被关闭");

        // 快路径获取 JdbcTemplate 依然正常工作
        JdbcTemplate jt = manager.getJdbcTemplate(null, mmsi);
        assertNotNull(jt);
        Integer res = jt.queryForObject("SELECT 1", Integer.class);
        assertEquals(1, res);
    }

    @Test
    @DisplayName("测试场景 12: ShipDatabase.toString masks password (元数据实体字符串表示中数据库密码必须被遮蔽)")
    void testShipDatabaseToStringMasksPassword() {
        ShipDataSourceManager.ShipDatabase db = new ShipDataSourceManager.ShipDatabase(
                "ship-1", "413999999", "zncb_ship", "localhost", 3306, "root", "super_secret_pass", true
        );
        String str = db.toString();
        assertNotNull(str);
        assertFalse(str.contains("super_secret_pass"), "日志与字符串表示绝对不能暴露明文密码");
        assertTrue(str.contains("password=******"), "密码必须被星号遮蔽");
    }

    @Test
    @DisplayName("测试场景 13: whitespace normalization and canonical routing (MMSI与shipId包含首尾空格时规范化，复用同一连接池且不泄漏)")
    void testWhitespaceNormalizationAndCanonicalRouting() {
        String mmsi = "413999999";
        String h2Url = "jdbc:h2:mem:ship_norm_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url, 0, "sa", "", true);

        JdbcTemplate jt1 = manager.getJdbcTemplate(null, " 413999999 ");
        JdbcTemplate jt2 = manager.getJdbcTemplate(null, "413999999");
        JdbcTemplate jt3 = manager.getJdbcTemplate("  ship-1  ", null);

        assertSame(jt1, jt2, "带空格的 MMSI 与标准 MMSI 获取的 JdbcTemplate 必须完全同一");
        assertSame(jt1, jt3, "带空格的 shipId 路由获取的 JdbcTemplate 必须完全同一");
        assertEquals(1, manager.listPoolSnapshots().size(), "底层只能存在 1 个 HikariCP 连接池");
    }

    @Test
    @DisplayName("测试场景 14: concurrent first access mixed shipId and MMSI (多线程混合以 shipId 与 mmsi 首次并发访问，仅保留 1 个连接池)")
    void testConcurrentFirstAccessMixedShipIdAndMmsi() throws InterruptedException {
        String mmsi = "413888888";
        String h2Url = "jdbc:h2:mem:ship_mixed_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-mixed", mmsi, "zncb_ship_mixed", h2Url, 0, "sa", "", true);

        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);

        List<JdbcTemplate> results = Collections.synchronizedList(new ArrayList<>());
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threadCount; i++) {
            final boolean useShipId = (i % 2 == 0);
            executor.submit(() -> {
                try {
                    startLatch.await();
                    JdbcTemplate jt = useShipId
                            ? manager.getJdbcTemplate("ship-mixed", null)
                            : manager.getJdbcTemplate(null, mmsi);
                    jt.execute("SELECT 1");
                    results.add(jt);
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(finishLatch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertTrue(errors.isEmpty(), "混合并发请求必须全部成功: " + errors);
        assertEquals(threadCount, results.size());

        JdbcTemplate first = results.get(0);
        for (JdbcTemplate jt : results) {
            assertSame(first, jt, "所有并发线程获取的 JdbcTemplate 必须完全相同");
        }
        assertEquals(1, manager.listPoolSnapshots().size(), "最终只能有 1 个存活连接池");
    }

    @Test
    @DisplayName("测试场景 15: shutdown safety rejects further operations (应用停机销毁后拒绝后续操作且不泄漏新连接池)")
    void testShutdownSafetyRejectsFurtherOperations() {
        String mmsi = "413999999";
        String h2Url = "jdbc:h2:mem:ship_shutdown_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-1", mmsi, "zncb_ship_1", h2Url, 0, "sa", "", true);

        manager.getJdbcTemplate(null, mmsi);
        assertEquals(1, manager.listPoolSnapshots().size());

        // 执行销毁
        manager.closeAll();

        // 销毁后调用 getJdbcTemplate 坚决拒绝并抛出 IllegalStateException，防止停机并发漏建连接池
        assertThrows(IllegalStateException.class, () -> manager.getJdbcTemplate(null, mmsi));
        assertThrows(IllegalStateException.class, () -> manager.refreshDataSource(mmsi));
        assertEquals(0, manager.listPoolSnapshots().size(), "销毁后绝对不允许残留连接池");
    }

    @Test
    @DisplayName("测试场景 16: shipId reassignment cleans old mapping (船舶变更 shipId 刷新后旧映射被清理)")
    void testShipIdReassignmentCleansOldMapping() {
        String mmsi = "413999999";
        String h2Url = "jdbc:h2:mem:ship_rename_" + mmsi + ";DB_CLOSE_DELAY=-1;MODE=MySQL";
        insertShip("ship-old", mmsi, "zncb_ship_1", h2Url, 0, "sa", "", true);

        // 初始获取
        JdbcTemplate jtOld = manager.getJdbcTemplate("ship-old", null);
        assertNotNull(jtOld);

        // 在主库中将 ship_id 更新为 ship-new，并修改 databaseName 触发刷新
        authJdbcTemplate.update("UPDATE ship_database_registry SET ship_id = 'ship-new', database_name = 'zncb_ship_new' WHERE mmsi = ?", mmsi);
        boolean refreshed = manager.refreshDataSource(mmsi);
        assertTrue(refreshed);

        // 使用新 shipId 获取正常
        JdbcTemplate jtNew = manager.getJdbcTemplate("ship-new", null);
        assertNotNull(jtNew);

        // 此时旧 shipId 如果在数据库中已无记录，调用将抛出 IllegalStateException（因为旧索引已清理）
        assertThrows(IllegalStateException.class, () -> manager.getJdbcTemplate("ship-old", null));
    }
}
