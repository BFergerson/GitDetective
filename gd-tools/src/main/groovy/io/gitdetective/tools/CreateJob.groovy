package io.gitdetective.tools

import io.gitdetective.web.dao.JobsDAO
import io.gitdetective.web.work.importer.GraknImporter
import io.vertx.blueprint.kue.queue.Priority
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
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
    static boolean skipFilter

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
        if (args.length > 3) {
            skipFilter = args[3] as boolean
        }

        Vertx.vertx().deployVerticle(new CreateJob(), new DeploymentOptions().setConfig(config), {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }
        })
    }

    @Override
    void start() throws Exception {
        def jobs = new JobsDAO(vertx, config())
        jobs.createJob(jobType, "Admin build job queued",
                new JsonObject().put("github_repository", projectName.toLowerCase())
                        .put("admin_triggered", true)
                        .put("skip_filter", skipFilter),
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
