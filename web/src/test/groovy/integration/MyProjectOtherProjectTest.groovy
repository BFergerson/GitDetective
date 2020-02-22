package integration

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.google.common.base.Charsets
import com.google.common.io.Resources
import groovy.util.logging.Slf4j
import io.gitdetective.web.GitDetectiveService
import io.gitdetective.web.model.FunctionInformation
import io.gitdetective.web.task.UpdateFunctionReferenceCounts
import io.vertx.core.*
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.ext.web.Router
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory

import java.time.Instant

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
    private static String myProjectId
    private static String otherProjectId
    private static String myProjectMyClassFileId
    private static String otherProjectApp2FileId
    private static String myProjectMyMethodId
    private static String otherProjectMainId
    private static FunctionInformation myProjectMyMethod
    private static FunctionInformation otherProjectMain

    @BeforeClass
    static void setUp(TestContext test) {
        vertx = Vertx.vertx()
        def deployOptions = new DeploymentOptions()
        deployOptions.setConfig(new JsonObject(new File("web-config.json").text))
        detectiveService = new GitDetectiveService(Router.router(vertx))

        def async = test.async()
        vertx.deployVerticle(detectiveService, deployOptions, {
            if (it.succeeded()) {
                detectiveService.postgres.client.query(
                        "SELECT 1 FROM information_schema.tables WHERE table_name = 'function_reference'", {
                    if (it.succeeded()) {
                        if (it.result().isEmpty()) {
                            detectiveService.postgres.client.query(Resources.toString(Resources.getResource(
                                    "reference-storage-schema.sql"), Charsets.UTF_8), {
                                if (it.succeeded()) {
                                    async.complete()
                                } else {
                                    test.fail(it.cause())
                                }
                            })
                        } else {
                            async.complete()
                        }
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
        log.info("Tearing down Vertx")
        vertx.close(test.asyncAssertSuccess())
    }

    @Test
    void doTest(TestContext test) {
        def importProjects = test.async()
        doMyProjectImport({
            if (it.succeeded()) {
                doOtherProjectImport({
                    if (it.succeeded()) {
                        detectiveService.projectService.insertFunctionReference(
                                otherProjectId, otherProjectApp2FileId, Instant.parse("2020-02-22T20:02:20Z"),
                                "b0be5053300ef5baabe7706f1cb440e38aa55565", 20,
                                otherProjectMain, myProjectMyMethod, {
                            if (it.succeeded()) {
                                vertx.eventBus().request(UpdateFunctionReferenceCounts.PERFORM_TASK_NOW, true, {
                                    if (it.succeeded()) {
                                        importProjects.complete()
                                    } else {
                                        test.fail(it.cause())
                                    }
                                })
                            } else {
                                test.fail(it.cause())
                            }
                        })
                    } else {
                        test.fail(it.cause())
                    }
                })
            } else {
                test.fail(it.cause())
            }
        })

        def testFinished = test.async()
        importProjects.handler({
            def async = test.async(6)
            detectiveService.projectService.getFileCount("github:bfergerson/myproject", {
                if (it.failed()) {
                    test.fail(it.cause())
                }
                test.assertEquals(1L, it.result())
                async.countDown()
            })
            detectiveService.projectService.getFileCount("github:bfergerson/otherproject", {
                if (it.failed()) {
                    test.fail(it.cause())
                }
                test.assertEquals(1L, it.result())
                async.countDown()
            })
            detectiveService.projectService.getFunctionCount("github:bfergerson/myproject", {
                if (it.failed()) {
                    test.fail(it.cause())
                }
                test.assertEquals(1L, it.result())
                async.countDown()
            })
            detectiveService.projectService.getFunctionCount("github:bfergerson/otherproject", {
                if (it.failed()) {
                    test.fail(it.cause())
                }
                test.assertEquals(1L, it.result())
                async.countDown()
            })
            detectiveService.projectService.getMostReferencedFunctionsInformation("github:bfergerson/myproject", 1, {
                if (it.failed()) {
                    test.fail(it.cause())
                }
                test.assertEquals(1, it.result().size())
                test.assertEquals("com.gitdetective.MyClass.myMethod()", it.result().get(0).qualifiedName)
                test.assertEquals(1, it.result().get(0).referenceCount)

                detectiveService.postgres.getFunctionReferences(it.result().get(0).functionId, 10, {
                    if (it.failed()) {
                        test.fail(it.cause())
                    }

                    test.assertEquals(1, it.result().size())
                    async.countDown()
                })
            })
            detectiveService.projectService.getMostReferencedFunctionsInformation("github:bfergerson/otherproject", 1, {
                if (it.failed()) {
                    test.fail(it.cause())
                }
                test.assertEquals(1, it.result().size())
                test.assertEquals("com.gitdetective.App2.main(java.lang.String[])", it.result().get(0).qualifiedName)
                test.assertEquals(0, it.result().get(0).referenceCount)
                async.countDown()
            })

            async.handler({
                testFinished.complete()
            })
        })
    }

    private static void doMyProjectImport(Handler<AsyncResult<Void>> handler) {
        def createProjectsAsync = Future.future()
        String userId = null
        detectiveService.userService.getOrCreateUser("github:bfergerson", {
            if (it.failed()) {
                createProjectsAsync.fail(it.cause())
            }
            userId = it.result()
            detectiveService.projectService.getOrCreateProject(userId, "github:bfergerson/myproject", {
                if (it.failed()) {
                    createProjectsAsync.fail(it.cause())
                }
                myProjectId = it.result()
                createProjectsAsync.complete()
            })
        })

        def createFilesAsync = Future.future()
        createProjectsAsync.setHandler({
            detectiveService.projectService.getOrCreateFile(myProjectId, "com.gitdetective.MyClass",
                    "src/main/java/com/gitdetective/MyClass.java", {
                if (it.succeeded()) {
                    myProjectMyClassFileId = it.result()
                    createFilesAsync.complete()
                } else {
                    createFilesAsync.fail(it.cause())
                }
            })
        })

        def createFunctionAsync = Future.future()
        createFilesAsync.setHandler({
            myProjectMyMethod = new FunctionInformation(
                    "kythe://kythe?lang=java?path=com/gitdetective/MyClass.java#6a69bc35ea8774d37510f3405373b2a65e029a4528321575191a00912b83818f",
                    "com.gitdetective.MyClass.myMethod()"
            )
            detectiveService.projectService.getOrCreateFunction(myProjectMyClassFileId, myProjectMyMethod, {
                if (it.succeeded()) {
                    myProjectMyMethodId = it.result()
                    createFunctionAsync.complete()
                } else {
                    createFunctionAsync.fail(it.cause())
                }
            })
        })
        CompositeFuture.all(createProjectsAsync, createFilesAsync, createFunctionAsync).setHandler(handler)
    }

    private static void doOtherProjectImport(Handler<AsyncResult<Void>> handler) {
        def createProjectsAsync = Future.future()
        String userId = null
        detectiveService.userService.getOrCreateUser("github:bfergerson", {
            if (it.failed()) {
                createProjectsAsync.fail(it.cause())
            }
            userId = it.result()
            detectiveService.projectService.getOrCreateProject(userId, "github:bfergerson/otherproject", {
                if (it.failed()) {
                    createProjectsAsync.fail(it.cause())
                }
                otherProjectId = it.result()
                createProjectsAsync.complete()
            })
        })

        def createFilesAsync = Future.future()
        createProjectsAsync.setHandler({
            detectiveService.projectService.getOrCreateFile(otherProjectId, "com.gitdetective.App2",
                    "src/main/java/com/gitdetective/App2.java", {
                if (it.succeeded()) {
                    otherProjectApp2FileId = it.result()
                    createFilesAsync.complete()
                } else {
                    createFilesAsync.fail(it.cause())
                }
            })
        })

        def createFunctionAsync = Future.future()
        createFilesAsync.setHandler({
            otherProjectMain = new FunctionInformation(
                    "kythe://kythe?lang=java?path=com/gitdetective/App2.java#d1986b43119e8013b76b5a57426d0ed51d3ef15a8bdb166657ef6aff91d3e6fc",
                    "com.gitdetective.App2.main(java.lang.String[])"
            )
            detectiveService.projectService.getOrCreateFunction(otherProjectApp2FileId, otherProjectMain, {
                if (it.succeeded()) {
                    otherProjectMainId = it.result()
                    createFunctionAsync.complete()
                } else {
                    createFunctionAsync.fail(it.cause())
                }
            })
        })
        CompositeFuture.all(createProjectsAsync, createFilesAsync, createFunctionAsync).setHandler(handler)
    }
}
