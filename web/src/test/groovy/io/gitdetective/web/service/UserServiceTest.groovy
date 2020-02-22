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
class UserServiceTest extends GitDetectiveServiceTest {

    private static UserService userService

    @BeforeClass
    static void setUp(TestContext test) {
        def async = test.async()
        setUp({
            if (it.succeeded()) {
                vertx.deployVerticle(userService = new UserService(graknSession), {
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
    void testGetOrCreateUser(TestContext test) {
        def async = test.async()
        userService.getOrCreateUser("github:bfergerson", {
            if (it.succeeded()) {
                def userId = it.result()
                test.assertNotNull(userId)

                userService.getOrCreateUser("github:bfergerson", {
                    if (it.succeeded()) {
                        test.assertEquals(userId, it.result())
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

    @Test
    void testGetProjectCount(TestContext test) {
        def async = test.async()
        userService.getProjectCount("github:bfergerson", {
            if (it.succeeded()) {
                test.assertEquals(2L, it.result())
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }

    @Test
    void testGetMostReferencedProjectsInformation(TestContext test) {
        def async = test.async()
        userService.getMostReferencedProjectsInformation("github:bfergerson", 2, {
            if (it.succeeded()) {
                test.assertEquals(2, it.result().size())
                test.assertEquals("github:bfergerson/myproject", it.result().get(0).projectName)
                test.assertEquals(1, it.result().get(0).referenceCount)
                test.assertEquals("github:bfergerson/otherproject", it.result().get(1).projectName)
                test.assertEquals(0, it.result().get(1).referenceCount)
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }
}
