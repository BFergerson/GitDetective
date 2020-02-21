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
class UserServiceTest {

    static {
        //disable grakn 'io.netty' DEBUG logging
        Logger root = (Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME)
        root.setLevel(Level.INFO)
    }

    private static Vertx vertx
    private static GraknClient client
    private static GraknClient.Session session
    private static UserService userService

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
        vertx.deployVerticle(userService = new UserService(session), {
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
