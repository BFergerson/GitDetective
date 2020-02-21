package integration

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import groovy.util.logging.Slf4j
import io.gitdetective.web.GitDetectiveService
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.ext.web.Router
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory

import static org.slf4j.Logger.ROOT_LOGGER_NAME

@Slf4j
@RunWith(VertxUnitRunner.class)
class MyProjectOtherProjectTest {
    static {
        //disable grakn 'io.netty' DEBUG logging
        Logger root = (Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME)
        root.setLevel(Level.INFO)
    }

    private static Vertx vertx
    private static GitDetectiveService detectiveService

    @BeforeClass
    static void setUp(TestContext test) {
        vertx = Vertx.vertx()
        def deployOptions = new DeploymentOptions()
        deployOptions.setConfig(new JsonObject(new File("web-config.json").text))

        detectiveService = new GitDetectiveService(Router.router(vertx))
        vertx.deployVerticle(detectiveService, deployOptions, test.asyncAssertSuccess())
    }

    @AfterClass
    static void tearDown(TestContext test) {
        log.info("Tearing down Vertx")
        vertx.close(test.asyncAssertSuccess())
    }

    @Test
    void importMyProject(TestContext test) {
        def createProjectAsync = test.async()
        detectiveService.userService.getOrCreateUser("github:bfergerson", {
            if (it.failed()) {
                test.fail(it.cause())
            }
            detectiveService.projectService.getOrCreateProject(it.result(), "github:bfergerson/myproject", {
                if (it.failed()) {
                    test.fail(it.cause())
                }
                createProjectAsync.complete()
            })
        })

        def testFinished = test.async()
        createProjectAsync.handler({
            testFinished.complete()
        })
    }
}
