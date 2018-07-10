package io.gitdetective.tools

import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.JobState
import io.vertx.blueprint.kue.queue.KueVerticle
import io.vertx.core.*
import io.vertx.core.json.JsonObject

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class MoveJobs extends AbstractVerticle {

    static JobState fromJobState
    static JobState toJobState

    static void main(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException("Invalid arguments: " + args.toArrayString())
        }

        //todo: no environment; do by local config
        def config
        if (args[0].toLowerCase() == "local") {
            config = new JsonObject().put("redis.host", "localhost").put("redis.port", 6379)
        } else {
            throw new IllegalArgumentException("Invalid environment: " + args[0])
        }
        fromJobState = JobState.valueOf(args[1].toUpperCase())
        toJobState = JobState.valueOf(args[2].toUpperCase())

        DeploymentOptions options = new DeploymentOptions().setConfig(config)
        VertxOptions vertxOptions = new VertxOptions()
        vertxOptions.setBlockedThreadCheckInterval(Integer.MAX_VALUE)
        Vertx vertx = Vertx.vertx(vertxOptions)

        vertx.deployVerticle(new KueVerticle(), options, {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }
            vertx.deployVerticle(new MoveJobs(), options, {
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
        kue.getIdsByState(fromJobState).setHandler({
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            } else {
                def futures = new ArrayList<Future>()
                it.result().each {
                    def fut = Future.future()
                    futures.add(fut)
                    kue.getJob(it).setHandler({
                        if (it.failed()) {
                            fut.fail(it.cause())
                        } else {
                            it.result().get().state(toJobState).setHandler(fut.completer())
                        }
                    })
                }
                CompositeFuture.all(futures).setHandler({
                    if (it.failed()) {
                        it.cause().printStackTrace()
                        System.exit(-1)
                    } else {
                        println "Moved " + it.result().size() + " jobs"
                        vertx.close()
                    }
                })
            }
        })
    }

}
