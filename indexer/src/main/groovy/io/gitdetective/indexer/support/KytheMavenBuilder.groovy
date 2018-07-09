package io.gitdetective.indexer.support

import io.gitdetective.indexer.GitDetectiveIndexer
import io.gitdetective.indexer.stage.GithubRepositoryCloner
import io.gitdetective.indexer.stage.KytheIndexOutput
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import org.apache.maven.shared.invoker.*

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import static io.gitdetective.web.Utils.asPrettyTime
import static io.gitdetective.web.Utils.logPrintln

/**
 * Builds Maven projects with Kythe attached
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class KytheMavenBuilder extends AbstractVerticle {

    public static final String BUILDER_ADDRESS = "KytheMavenBuilder"
    private static final long GB_BYTES = 1_073_741_824L
    private static final File repoDir = new File("/tmp/.m2")
    private static final File javacWrapper = new File("opt/kythe-v0.0.26/extractors/javac-wrapper.sh")
    private static final File javacExtractor = new File("opt/kythe-v0.0.26/extractors/javac_extractor.jar")
    private static final File mavenHome = new File("opt/builders/apache-maven-3.5.3")

    @Override
    void start() throws Exception {
        repoDir.mkdirs()

        vertx.eventBus().consumer(BUILDER_ADDRESS, { msg ->
            vertx.executeBlocking({
                //check m2 size; empty if too big
                if (repoDir.directorySize() / GB_BYTES >= config().getInteger("local_maven_repository_max_size_gb")) {
                    println "Cleaning local Maven repo: " + repoDir.absolutePath
                    repoDir.deleteDir()
                    repoDir.mkdirs()
                }
                it.complete()
            }, {
                //process job
                def job = (Job) msg.body()
                buildProject(job, new File(job.data.getString("build_target")))
            })
        })
    }

    private void buildProject(Job job, File pomFile) {
        InvocationRequest request = new DefaultInvocationRequest()
        request.setLocalRepositoryDirectory(repoDir)
        request.setPomFile(pomFile)
        request.setGoals(Arrays.asList("clean", "compile"))
        request.setBatchMode(true)

        def kytheDir = new File(job.data.getString("output_directory"), "kythe")
        kytheDir.mkdirs()

        request.addShellEnvironment("REAL_JAVAC", "/usr/bin/javac")
        request.addShellEnvironment("KYTHE_ROOT_DIRECTORY", pomFile.parentFile.absolutePath)
        request.addShellEnvironment("KYTHE_OUTPUT_DIRECTORY", kytheDir.absolutePath)
        request.addShellEnvironment("JAVAC_EXTRACTOR_JAR", javacExtractor.absolutePath)

        def props = new Properties()
        //props.setProperty("T", "1C") //one thread per core
        props.setProperty("skip.npm", "true")
        props.setProperty("skip.yarn", "true") //todo: don't think these do anything
        props.setProperty("skip.bower", "true")
        props.setProperty("skip.grunt", "true")
        props.setProperty("skip.gulp", "true")
        props.setProperty("skip.jspm", "true")
        props.setProperty("skip.karma", "true")
        props.setProperty("skip.webpack", "true")
        props.setProperty("findbugs.skip", "true")
        props.setProperty("pmd.skip", "true")
        props.setProperty("checkstyle.skip", "true")
        props.setProperty("maven.javadoc.skip", "true")
        props.setProperty("maven.test.skip", "true")
        props.setProperty("maven.compiler.target", "1.8")
        props.setProperty("maven.compiler.source", "1.8")
        props.setProperty("maven.compiler.forceJavacCompilerUse", "true")
        props.setProperty("maven.compiler.fork", "true")
        props.setProperty("maven.compiler.executable", javacWrapper.absolutePath)
        request.setProperties(props)

        final ExecutorService service = Executors.newFixedThreadPool(1)
        final ScheduledExecutorService canceller = Executors.newSingleThreadScheduledExecutor()
        Invoker invoker = new DefaultInvoker()
        invoker.setMavenHome(mavenHome.absoluteFile)

        def buildTimer = GitDetectiveIndexer.metrics.timer("BuildMavenProject")
        def buildContext = buildTimer.time()
        vertx.executeBlocking({ future ->
            def buildTimeoutFuture = service.submit(new Runnable() {
                @Override
                void run() {
                    logPrintln(job, "Building project")
                    def result = invoker.execute(request)
                    future.complete(result)
                }
            })
            canceller.schedule(new Runnable() {
                @Override
                void run() {
                    if (!buildTimeoutFuture.done) {
                        buildTimeoutFuture.cancel(true)
                        logPrintln(job, "Project build timed out")
                        job.failed()
                        vertx.eventBus().send(GithubRepositoryCloner.PROCESS_NEXT_JOB, new JsonObject())
                    }
                }
            }, 1, TimeUnit.HOURS)
        }, false, { res ->
            def invocationResult = res.result() as InvocationResult
            if (invocationResult.exitCode != 0) {
                if (invocationResult.executionException != null) {
                    invocationResult.executionException.printStackTrace()
                    logPrintln(job, invocationResult.executionException.message)
                }

                logPrintln(job, "Project build failed")
                job.failed()
                vertx.eventBus().send(GithubRepositoryCloner.PROCESS_NEXT_JOB, new JsonObject())
            } else {
                logPrintln(job, "Project build took: " + asPrettyTime(buildContext.stop()))
                vertx.eventBus().send(KytheIndexOutput.KYTHE_INDEX_OUTPUT, job)
            }
        })
    }

}
