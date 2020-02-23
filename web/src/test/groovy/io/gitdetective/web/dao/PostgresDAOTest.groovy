package io.gitdetective.web.dao

import groovy.util.logging.Slf4j
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.pgclient.PgConnectOptions
import io.vertx.pgclient.PgPool
import io.vertx.sqlclient.PoolOptions
import org.junit.After
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith

import java.time.Instant
import java.time.temporal.ChronoUnit

@Slf4j
@RunWith(VertxUnitRunner.class)
class PostgresDAOTest {

    private static PgPool client
    private static PostgresDAO postgres

    @BeforeClass
    static void setUp(TestContext test) {
        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(5432)
                .setHost("localhost")
                .setDatabase("postgres")
                .setUser("postgres")
                .setPassword("postgres")
        PoolOptions poolOptions = new PoolOptions().setMaxSize(5)
        client = PgPool.pool(connectOptions, poolOptions)
        postgres = new PostgresDAO(client, test.asyncAssertSuccess())
    }

    @AfterClass
    static void tearDown() {
        client.close()
    }

    @After
    void clearDatabase(TestContext test) {
        client.query("DELETE FROM function_reference", test.asyncAssertSuccess())
    }

    @Test
    void testGetLiveProjectReferenceTrendOneHour(TestContext test) {
        def insertAsync = test.async(10)
        def now = Instant.now()
        postgres.insertFunctionReference(
                "V1", "V2", "V3", "sha1", now, 10, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.insertFunctionReference(
                "V1", "V4", "V3", "sha2", now, 10, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.insertFunctionReference(
                "V1", "V5", "V3", "sha3", now, 10, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.insertFunctionReference(
                "V1", "V6", "V3", "sha4", now, 10, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.insertFunctionReference(
                "V1", "V7", "V3", "sha5", now, 10, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.removeFunctionReference(
                "V1", "V7", "V3", "sha6", now, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.removeFunctionReference(
                "V1", "V6", "V3", "sha7", now, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.removeFunctionReference(
                "V1", "V5", "V3", "sha8", now, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.removeFunctionReference(
                "V1", "V4", "V3", "sha9", now, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.removeFunctionReference(
                "V1", "V2", "V3", "sha10", now, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })

        def async = test.async()
        insertAsync.handler({
            postgres.getLiveProjectReferenceTrend(["V3"], {
                if (it.succeeded()) {
                    def trend = it.result()
                    test.assertEquals(2, trend.trendData.size())
                    test.assertEquals(false, trend.trendData.get(0).deletion)
                    test.assertEquals(5L, trend.trendData.get(0).count)
                    test.assertEquals(true, trend.trendData.get(1).deletion)
                    test.assertEquals(5L, trend.trendData.get(1).count)
                    test.assertTrue(trend.trendData.get(0).time == trend.trendData.get(1).time)
                    async.complete()
                } else {
                    test.fail(it.cause())
                }
            })
        })
    }

    @Test
    void testGetLiveProjectReferenceTrendTwoHours(TestContext test) {
        def insertAsync = test.async(10)
        def now = Instant.now()
        postgres.insertFunctionReference(
                "V1", "V2", "V3", "sha1", now, 10, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.insertFunctionReference(
                "V1", "V4", "V3", "sha2", now, 10, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.insertFunctionReference(
                "V1", "V5", "V3", "sha3", now, 10, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.insertFunctionReference(
                "V1", "V6", "V3", "sha4", now, 10, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.insertFunctionReference(
                "V1", "V7", "V3", "sha5", now, 10, {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.removeFunctionReference(
                "V1", "V7", "V3", "sha6", now.plus(1, ChronoUnit.HOURS), {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.removeFunctionReference(
                "V1", "V6", "V3", "sha7", now.plus(1, ChronoUnit.HOURS), {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.removeFunctionReference(
                "V1", "V5", "V3", "sha8", now.plus(1, ChronoUnit.HOURS), {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.removeFunctionReference(
                "V1", "V4", "V3", "sha9", now.plus(1, ChronoUnit.HOURS), {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })
        postgres.removeFunctionReference(
                "V1", "V2", "V3", "sha10", now.plus(1, ChronoUnit.HOURS), {
            if (it.failed()) {
                test.fail(it.cause())
            }
            insertAsync.countDown()
        })

        def async = test.async()
        insertAsync.handler({
            postgres.getLiveProjectReferenceTrend(["V3"], {
                if (it.succeeded()) {
                    def trend = it.result()
                    test.assertEquals(2, trend.trendData.size())
                    test.assertEquals(false, trend.trendData.get(0).deletion)
                    test.assertEquals(5L, trend.trendData.get(0).count)
                    test.assertEquals(true, trend.trendData.get(1).deletion)
                    test.assertEquals(5L, trend.trendData.get(1).count)
                    test.assertTrue(trend.trendData.get(0).time < trend.trendData.get(1).time)
                    async.complete()
                } else {
                    test.fail(it.cause())
                }
            })
        })
    }
}
