package io.gitdetective.tools

import io.gitdetective.web.dao.JobsDAO
import io.vertx.blueprint.kue.queue.Priority
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
            jobType = "IndexGithubProject"
        } else if (jobType == "import") {
           // jobType = GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE
        } else {
            throw new IllegalArgumentException("Invalid job type: " + jobType)
        }
        priority = args[1]
        projectsFile = args[2]
        if (!new File(projectsFile).exists()) {
            throw new IllegalStateException("File does not exists: " + projectsFile)
        }

        def vertxOptions = new VertxOptions()
        vertxOptions.setBlockedThreadCheckInterval(Integer.MAX_VALUE)
        Vertx.vertx(vertxOptions).deployVerticle(new CreateJobs(), new DeploymentOptions().setConfig(config), {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }
        })
    }

    @Override
    void start() throws Exception {
        def jobsFut = Future.future()
        def jobs = new JobsDAO(vertx, config(), jobsFut.completer())

        jobsFut.setHandler({
            def futures = new ArrayList<Future>()
            new File(projectsFile).eachLine {
                def fut = Future.future()
                futures.add(fut)

                jobs.createJob(jobType, "Admin build job queued",
                        new JsonObject().put("github_repository", it.toLowerCase()).put("admin_triggered", true),
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
        })
    }
}
