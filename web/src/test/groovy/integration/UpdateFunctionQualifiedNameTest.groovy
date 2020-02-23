package integration

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.google.common.base.Charsets
import com.google.common.io.Resources
import groovy.util.logging.Slf4j
import io.gitdetective.web.GitDetectiveService
import io.gitdetective.web.model.FunctionInformation
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

import java.time.Instant

import static org.slf4j.Logger.ROOT_LOGGER_NAME

@Slf4j
@RunWith(VertxUnitRunner.class)
class UpdateFunctionQualifiedNameTest {
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

        def async = test.async()
        vertx.deployVerticle(detectiveService, deployOptions, {
            if (it.succeeded()) {
                if (it.succeeded()) {
                    async.complete()
                } else {
                    test.fail(it.cause())
                }
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
        MyProjectOtherProjectTest.doOtherProjectImport(detectiveService, {
            if (it.succeeded()) {
                def myMethodIncorrect = new FunctionInformation(
                        "kythe://kythe?lang=java?path=com/gitdetective/MyClass.java#6a69bc35ea8774d37510f3405373b2a65e029a4528321575191a00912b83818f",
                        "com.gitdetective.MyClass.myMethod_incorrect()"
                )
                detectiveService.projectService.insertFunctionReference(
                        MyProjectOtherProjectTest.otherProjectId, MyProjectOtherProjectTest.otherProjectApp2FileId,
                        Instant.parse("2020-02-22T20:02:20Z"),
                        "b0be5053300ef5baabe7706f1cb440e38aa55565", 20,
                        MyProjectOtherProjectTest.otherProjectMain, myMethodIncorrect, {
                    if (it.succeeded()) {
                        //test update grakn before myProject import
                        MyProjectOtherProjectTest.doGraknUpdateWork(vertx, {
                            if (it.succeeded()) {
                                MyProjectOtherProjectTest.doMyProjectImport(detectiveService, {
                                    if (it.succeeded()) {
                                        //test update grakn after myProject import
                                        MyProjectOtherProjectTest.doGraknUpdateWork(vertx, {
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
}
