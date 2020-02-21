package io.gitdetective.web.service

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.google.common.base.Charsets
import com.google.common.io.Resources
import grakn.client.GraknClient
import graql.lang.Graql
import groovy.util.logging.Slf4j
import io.gitdetective.web.GitDetectiveService
import io.vertx.core.Vertx
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory

import static org.slf4j.Logger.ROOT_LOGGER_NAME

@Slf4j
@RunWith(VertxUnitRunner.class)
class ProjectServiceTest {

    static {
        //disable grakn 'io.netty' DEBUG logging
        Logger root = (Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME)
        root.setLevel(Level.INFO)
    }

    private static Vertx vertx
    private static GraknClient client
    private static GraknClient.Session session
    private static ProjectService projectService

    @BeforeClass
    static void setUp(TestContext test) {
        String graknHost = "localhost"
        int graknPort = 48555
        String graknKeyspace = "grakn"
        client = new GraknClient("$graknHost:$graknPort")
        try {
            session = client.session(graknKeyspace)
        } catch (all) {
            all.printStackTrace()
            throw new ConnectException("Connection refused: $graknHost:$graknPort")
        }
        GitDetectiveService.setupOntology(session)

        log.info("Loading test data")
        try {
            def tx = session.transaction().write()
            tx.execute(Graql.parse(Resources.toString(Resources.getResource(
                    "test-data.gql"), Charsets.UTF_8)))
            tx.commit()
            tx.close()
        } catch (all) {
            test.fail(all)
        }
        log.info("Loaded test data")

        Async async = test.async()
        vertx = Vertx.vertx()
        vertx.deployVerticle(projectService = new ProjectService(session), {
            if (it.succeeded()) {
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }

    @AfterClass
    static void tearDown(TestContext test) {
        log.info("Tearing down Vertx")
        def tx = session.transaction().write();
        tx.execute(Graql.match(Graql.var("x").isa("thing")).delete("x"))
        tx.commit()
        tx.close()
        session.close()
        client.close()
        vertx.close(test.asyncAssertSuccess())
    }

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
