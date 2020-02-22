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
class ProjectServiceTest extends GitDetectiveServiceTest {

    private static ProjectService projectService

    @BeforeClass
    static void setUp(TestContext test) {
        def async = test.async()
        setUp({
            if (it.succeeded()) {
                vertx.deployVerticle(projectService = new ProjectService(graknSession, postgres), {
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

//    @Test
//    void testGetOrCreateProject(TestContext test) {
//        def async = test.async()
//        projectService.getOrCreateProject("github:bfergerson/testproject", {
//            if (it.succeeded()) {
//                test.assertNotNull(it.result())
//                async.complete()
//            } else {
//                test.fail(it.cause())
//            }
//        })
//    }

    @Test
    void testGetProjectId(TestContext test) {
        def async = test.async()
        projectService.getProjectId("github:bfergerson/myproject", {
            if (it.succeeded()) {
                test.assertNotNull(it.result())
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }

    @Test
    void testGetProjectId_invalid(TestContext test) {
        def async = test.async()
        projectService.getProjectId("github:invalid/invalid", {
            if (it.succeeded()) {
                test.assertNull(it.result())
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }

    @Test
    void testGetFileCount(TestContext test) {
        def async = test.async()
        projectService.getFileCount("github:bfergerson/myproject", {
            if (it.succeeded()) {
                test.assertEquals(1L, it.result())
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }

    @Test
    void testGetFunctionCount(TestContext test) {
        def async = test.async()
        projectService.getFunctionCount("github:bfergerson/myproject", {
            if (it.succeeded()) {
                test.assertEquals(1L, it.result())
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }

    @Test
    void testGetMostReferencedFunctionsInformation(TestContext test) {
        def async = test.async()
        projectService.getMostReferencedFunctionsInformation("github:bfergerson/myproject", 1, {
            if (it.succeeded()) {
                test.assertEquals(1, it.result().size())
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }
}
