package io.gitdetective.web.service

import groovy.util.logging.Slf4j
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith

@Slf4j
@RunWith(VertxUnitRunner.class)
class SystemServiceTest extends GitDetectiveServiceTest {

    private static SystemService systemService

    @BeforeClass
    static void setUp(TestContext test) {
        def async = test.async()
        setUp({
            if (it.succeeded()) {
                vertx.deployVerticle(systemService = new SystemService(graknSession, postgres), {
                    if (it.succeeded()) {
                        async.complete()
                    } else {
                        test.fail(it.cause())
                    }
                })
            } else {
                test.fail(it.cause())
            }
        })
    }

    @AfterClass
    static void tearDown(TestContext test) {
        GitDetectiveServiceTest.tearDown(test)
    }

    @Test
    void testGetTotalProjectCount(TestContext test) {
        def async = test.async()
        systemService.getTotalFileCount({
            if (it.succeeded()) {
                test.assertEquals(2L, it.result())
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }

    @Test
    void testGetTotalFileCount(TestContext test) {
        def async = test.async()
        systemService.getTotalFileCount({
            if (it.succeeded()) {
                test.assertEquals(2L, it.result())
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }

    @Test
    void testGetTotalFunctionCount(TestContext test) {
        def async = test.async()
        systemService.getTotalFunctionCount({
            if (it.succeeded()) {
                test.assertEquals(2L, it.result())
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }
}
