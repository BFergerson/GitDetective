package io.gitdetective.tools

import io.gitdetective.indexer.stage.GithubRepositoryCloner
import io.gitdetective.web.dao.JobsDAO
import io.gitdetective.web.dao.RedisDAO
import io.gitdetective.web.work.calculator.GraknCalculator
import io.gitdetective.web.work.importer.GraknImporter
import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.KueVerticle
import io.vertx.blueprint.kue.queue.Priority
import io.vertx.blueprint.kue.util.RedisHelper
import io.vertx.core.*
import io.vertx.core.json.JsonObject
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class CreateJobs extends AbstractVerticle {

    static String jobType
    static String priority = "NORMAL"
    static String projectsFile

    static void main(String[] args) {
        def configFile = new File("web-config.json")
        if (!configFile.exists()) {
            throw new IllegalStateException("Missing web-config.json")
        } else if (args.length < 3) {
            throw new IllegalArgumentException("Invalid arguments: " + args.toArrayString())
        }

        def config = new JsonObject(IOUtils.toString(configFile.newInputStream(), StandardCharsets.UTF_8))
        jobType = args[0].toLowerCase()
        if (jobType == "index") {
            jobType = GithubRepositoryCloner.INDEX_GITHUB_PROJECT_JOB_TYPE
        } else if (jobType == "calculate") {
            jobType = GraknCalculator.GRAKN_CALCULATE_JOB_TYPE
        } else if (jobType == "import") {
            jobType = GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE
        } else {
            throw new IllegalArgumentException("Invalid job type: " + jobType)
        }
        priority = args[1]
        projectsFile = args[2]
        if (!new File(projectsFile).exists()) {
            throw new IllegalStateException("File does not exists: " + projectsFile)
        }

        DeploymentOptions options = new DeploymentOptions().setConfig(config)
        VertxOptions vertxOptions = new VertxOptions()
        vertxOptions.setBlockedThreadCheckInterval(Integer.MAX_VALUE)
        Vertx vertx = Vertx.vertx(vertxOptions)
        def kueOptions = new DeploymentOptions().setConfig(config)
        if (config.getJsonObject("jobs_server") != null) {
            kueOptions.config = config.getJsonObject("jobs_server")
        }

        vertx.deployVerticle(new KueVerticle(), kueOptions, {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }

            def kue = Kue.createQueue(vertx, kueOptions.config)
            vertx.deployVerticle(new CreateJobs(kue), options, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    System.exit(-1)
                }
            })
        })
    }

    private final Kue kue

    CreateJobs(Kue kue) {
        this.kue = kue
    }

    @Override
    void start() throws Exception {
        def redisClient = RedisHelper.client(vertx, config())
        def redis = new RedisDAO(redisClient)
        def jobs = new JobsDAO(kue, redis)

        def futures = new ArrayList<Future>()
        new File(projectsFile).eachLine {
            def fut = Future.future()
            futures.add(fut)

            def initialMessage = "Admin build job queued"
            if (jobType == "CalculateGithubProject") {
                initialMessage = "Admin reference recalculation queued"
            }
            jobs.createJob(jobType, initialMessage,
                    new JsonObject().put("github_repository", it)
                            .put("is_recalculation", jobType == GraknCalculator.GRAKN_CALCULATE_JOB_TYPE)
                            .put("admin_triggered", true),
                    Priority.valueOf(priority.toUpperCase()), {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    System.exit(-1)
                }

                println "Created job: " + it.result().getId()
                fut.complete()
            })
        }

        CompositeFuture.all(futures).setHandler({
            vertx.close()
        })
    }

}
