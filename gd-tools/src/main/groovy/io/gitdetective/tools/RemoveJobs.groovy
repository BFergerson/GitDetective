package io.gitdetective.tools

import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.JobState
import io.vertx.blueprint.kue.queue.KueVerticle
import io.vertx.core.*
import io.vertx.core.json.JsonObject
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class RemoveJobs extends AbstractVerticle {

    static JobState jobState

    static void main(String[] args) {
        def configFile = new File("web-config.json")
        if (!configFile.exists()) {
            throw new IllegalStateException("Missing web-config.json")
        } else if (args.length < 1) {
            throw new IllegalArgumentException("Invalid arguments: " + args.toArrayString())
        }

        def config = new JsonObject(IOUtils.toString(configFile.newInputStream(), StandardCharsets.UTF_8))
        jobState = JobState.valueOf(args[0].toUpperCase())

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
            vertx.deployVerticle(new RemoveJobs(), kueOptions, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    System.exit(-1)
                }
            })
        })
    }

    @Override
    void start() throws Exception {
        def kue = Kue.createQueue(vertx, config())
        kue.getIdsByState(jobState).setHandler({
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            } else {
                def futures = new ArrayList<Future>()
                it.result().each {
                    def fut = Future.future()
                    futures.add(fut)
                    kue.removeJob(it).setHandler(fut.completer())
                }
                CompositeFuture.all(futures).setHandler({
                    if (it.failed()) {
                        it.cause().printStackTrace()
                        System.exit(-1)
                    } else {
                        println "Removed " + it.result().size() + " jobs"
                        vertx.close()
                    }
                })
            }
        })
    }
}
