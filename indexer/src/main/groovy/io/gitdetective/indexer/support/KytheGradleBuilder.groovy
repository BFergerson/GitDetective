package io.gitdetective.indexer.support

import io.gitdetective.indexer.GitDetectiveIndexer
import io.gitdetective.indexer.stage.KytheIndexOutput
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import org.gradle.tooling.BuildLauncher
import org.gradle.tooling.GradleConnector
import org.gradle.tooling.ProjectConnection
import org.gradle.tooling.internal.consumer.DefaultCancellationTokenSource

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import static io.gitdetective.indexer.IndexerServices.asPrettyTime
import static io.gitdetective.indexer.IndexerServices.logPrintln

/**
 * Builds Gradle projects with Kythe attached
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class KytheGradleBuilder extends AbstractVerticle {

    public static final String BUILDER_ADDRESS = "KytheGradleBuilder"
    private final static Logger log = LoggerFactory.getLogger(KytheGradleBuilder.class)
    private static final File repoDir = new File("/tmp/.gradle")
    private static final File javacWrapper = new File("opt/kythe-v0.0.28/extractors/javac-wrapper.sh")
    private static final File gradleHome = new File("opt/builders/gradle-4.9")
    private static final File javacExtractor = new File("opt/kythe-v0.0.28/extractors/javac_extractor.jar")

    @Override
    void start() throws Exception {
        repoDir.mkdirs()

        vertx.eventBus().consumer(BUILDER_ADDRESS, { msg ->
            def job = (Job) msg.body()
            buildProject(job, new File(job.data.getString("build_target")))
        })
        log.info "KytheGradleBuilder started"
    }

    private void buildProject(Job job, File buildFile) {
        logPrintln(job, "Setting up Gradle build")

        //basic gradle support for Java projects; todo: better
        buildFile.append("\nallprojects {\n" +
                "  gradle.projectsEvaluated {\n" +
                "    tasks.withType(JavaCompile) {\n" +
                "      options.encoding = \"UTF-8\"\n" +
                "      options.fork = true\n" +
                "      options.forkOptions.executable = '" + javacWrapper.absolutePath + "'\n" +
                "    }\n" +
                "  }\n" +
                "}")

        def buildTimer = GitDetectiveIndexer.metrics.timer("BuildGradleProject")
        def buildContext = buildTimer.time()
        ProjectConnection connection = GradleConnector.newConnector()
                .useInstallation(gradleHome)
                .useGradleUserHomeDir(repoDir)
                .forProjectDirectory(buildFile.parentFile)
                .connect()
        try {
            BuildLauncher build = connection.newBuild()
            build.forTasks("build")
            build.withArguments("-x", "test")
            build.setStandardOutput(System.out)

            def kytheDir = new File(job.data.getString("output_directory"), "kythe")
            kytheDir.mkdirs()

            def env = new HashMap<String, String>()
            env.put("REAL_JAVAC", "/usr/bin/javac")
            env.put("KYTHE_ROOT_DIRECTORY", buildFile.parentFile.absolutePath)
            env.put("KYTHE_OUTPUT_DIRECTORY", kytheDir.absolutePath)
            env.put("JAVAC_EXTRACTOR_JAR", javacExtractor.absolutePath)
            env.put("CI", "true") //todo: does this do anything?
            build.setEnvironmentVariables(env)

            def cancelSource = new DefaultCancellationTokenSource()
            final ExecutorService service = Executors.newFixedThreadPool(1)
            final ScheduledExecutorService canceller = Executors.newSingleThreadScheduledExecutor()
            vertx.executeBlocking({ future ->
                def buildTimeoutFuture = service.submit(new Runnable() {
                    @Override
                    void run() {
                        try {
                            logPrintln(job, "Building project")
                            build.withCancellationToken(cancelSource.token())
                            build.run()
                            connection.close()
                            future.complete()
                        } catch (all) {
                            cancelSource.cancel()
                            connection.close()
                            logPrintln(job, "Project build failed")
                            job.done(all)
                            future.fail(all)
                        }
                    }
                })
                canceller.schedule(new Runnable() {
                    @Override
                    void run() {
                        if (!buildTimeoutFuture.done) {
                            buildTimeoutFuture.cancel(true)
                            logPrintln(job, "Project build timed out")
                        }
                    }
                }, config().getJsonObject("builder_limits").getInteger("gradle_build_limit"), TimeUnit.MINUTES)
            }, false, { res ->
                if (res.succeeded()) {
                    logPrintln(job, "Project build took: " + asPrettyTime(buildContext.stop()))
                    vertx.eventBus().send(KytheIndexOutput.KYTHE_INDEX_OUTPUT, job)
                }
            })
        } catch (all) {
            all.printStackTrace()
            job.done(all)
            connection.close()
        }
    }

}
