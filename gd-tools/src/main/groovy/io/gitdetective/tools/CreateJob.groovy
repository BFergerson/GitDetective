package io.gitdetective.tools

import io.gitdetective.web.dao.JobsDAO
import io.gitdetective.web.dao.RedisDAO
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
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class CreateJob extends AbstractVerticle {

    static String jobType
    static String projectName
    static String priority = "NORMAL"

    static void main(String[] args) {
        def configFile = new File("web-config.json")
        if (!configFile.exists()) {
            throw new IllegalStateException("Missing web-config.json")
        } else if (args.length < 2) {
            throw new IllegalArgumentException("Invalid arguments: " + args.toArrayString())
        }

        def config = new JsonObject(IOUtils.toString(configFile.newInputStream(), StandardCharsets.UTF_8))
        jobType = args[0].toLowerCase()
        if (jobType == "index") {
            jobType = "IndexGithubProject"
        } else if (jobType == "import") {
            jobType = GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE
        } else {
            throw new IllegalArgumentException("Invalid job type: " + jobType)
        }

        projectName = args[1]
        if (args.length > 2) {
            priority = args[2]
        }

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
            vertx.deployVerticle(new CreateJob(kue), kueOptions, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    System.exit(-1)
                }
            })
        })
    }

    private final Kue kue

    CreateJob(Kue kue) {
        this.kue = kue
    }

    @Override
    void start() throws Exception {
        def jobsRedisConfig = config().copy()
        if (config().getJsonObject("jobs_server") != null) {
            jobsRedisConfig = config().getJsonObject("jobs_server")
        }
        def redisClient = RedisHelper.client(vertx, jobsRedisConfig)
        def redis = new RedisDAO(redisClient)
        def jobs = new JobsDAO(kue, redis)
        def initialMessage = "Admin build job queued"

        jobs.createJob(jobType, initialMessage,
                new JsonObject().put("github_repository", projectName.toLowerCase()).put("admin_triggered", true),
                Priority.valueOf(priority.toUpperCase()), {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }

            println "Created job: " + it.result().getId()
            vertx.close()
        })
    }

}
