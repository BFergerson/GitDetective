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
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.JsonObject

/**
 * todo: description
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class CreateJob extends AbstractVerticle {

    static String jobType
    static String projectName
    static String priority = "NORMAL"
    static boolean skipBuild = false

    static void main(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException("Invalid arguments: " + args.toArrayString())
        }

        def config
        if (args[0].toLowerCase() == "local") {
            config = new JsonObject().put("redis.host", "localhost").put("redis.port", 6379)
        } else {
            throw new IllegalArgumentException("Invalid environment: " + args[0])
        }
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

        projectName = args[2]
        if (args.length > 3) {
            priority = args[3]
        }
        if (args.length > 4) {
            skipBuild = args[4] as boolean
        }
        if (!skipBuild && jobType == GraknCalculator.GRAKN_CALCULATE_JOB_TYPE) {
            skipBuild = true
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
            vertx.deployVerticle(new CreateJob(), options, {
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
        def initialMessage = "Admin build job queued"
        if (jobType == "CalculateGithubProject") {
            initialMessage = "Admin reference/copy recalculation queued"
        }

        jobs.createJob(jobType, initialMessage,
                new JsonObject().put("github_repository", projectName)
                        .put("is_recalculation", jobType == GraknCalculator.GRAKN_CALCULATE_JOB_TYPE)
                        .put("build_skipped", skipBuild).put("admin_triggered", true),
                Priority.valueOf(priority.toUpperCase()), {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-5)
            }

            println "Created job: " + it.result().getId()
            vertx.close()
        })
    }

}
