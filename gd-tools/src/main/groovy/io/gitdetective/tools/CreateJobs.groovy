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

/**
 * todo: description
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class CreateJobs extends AbstractVerticle {

    static String jobType
    static String priority = "NORMAL"
    static String projectsFile

    static void main(String[] args) {
        if (args.length < 4) {
            throw new IllegalArgumentException("Invalid arguments: " + args.toArrayString())
        }

        def config = new JsonObject().put("redis.host", "localhost").put("redis.port", 6379)
        jobType = args[1].toLowerCase()
        if (jobType == "index") {
            jobType = GithubRepositoryCloner.INDEX_GITHUB_PROJECT_JOB_TYPE
        } else if (jobType == "calculate") {
            jobType = GraknCalculator.GRAKN_CALCULATE_JOB_TYPE
        } else if (jobType == "import") {
            jobType = GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE
        } else {
            throw new IllegalArgumentException("Invalid job type: " + jobType)
        }
        priority = args[2]
        projectsFile = args[3]
        if (!new File(projectsFile).exists()) {
            throw new IllegalStateException("File does not exists: " + projectsFile)
        }

        DeploymentOptions options = new DeploymentOptions().setConfig(config)
        VertxOptions vertxOptions = new VertxOptions()
        vertxOptions.setBlockedThreadCheckInterval(Integer.MAX_VALUE)
        Vertx vertx = Vertx.vertx(vertxOptions)

        vertx.deployVerticle(new KueVerticle(), options, {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-3)
            }
            vertx.deployVerticle(new CreateJobs(), options, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    System.exit(-4)
                }
            })
        })
    }

    @Override
    void start() throws Exception {
        def redisClient = RedisHelper.client(vertx, config())
        def redis = new RedisDAO(redisClient)
        def kue = Kue.createQueue(vertx, config())
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
                    System.exit(-5)
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
