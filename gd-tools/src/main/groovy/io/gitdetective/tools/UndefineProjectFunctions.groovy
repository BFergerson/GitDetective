package io.gitdetective.tools

import io.gitdetective.web.dao.PostgresDAO
import io.gitdetective.web.dao.RedisDAO
import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.KueVerticle
import io.vertx.blueprint.kue.util.RedisHelper
import io.vertx.core.*
import io.vertx.core.json.JsonObject
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class UndefineProjectFunctions extends AbstractVerticle {

    static String projectName
    static String qualifiedNamePattern

    static void main(String[] args) {
        def configFile = new File("web-config.json")
        if (!configFile.exists()) {
            throw new IllegalStateException("Missing web-config.json")
        } else if (args.length < 2) {
            throw new IllegalArgumentException("Invalid arguments: " + args.toArrayString())
        }

        def config = new JsonObject(IOUtils.toString(configFile.newInputStream(), StandardCharsets.UTF_8))
        projectName = args[0]
        qualifiedNamePattern = args[1]

        Vertx vertx = Vertx.vertx()
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
            vertx.deployVerticle(new UndefineProjectFunctions(kue), new DeploymentOptions().setConfig(config), {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    System.exit(-1)
                }
            })
        })
    }

    private final Kue kue

    UndefineProjectFunctions(Kue kue) {
        this.kue = kue
    }

    @Override
    void start() throws Exception {
        def redis = new RedisDAO(RedisHelper.client(vertx, config()))
        def refStorage = redis
        if (config().getJsonObject("storage") != null) {
            refStorage = new PostgresDAO(vertx, config().getJsonObject("storage"), redis)
        }

        refStorage.getOwnedFunctions(projectName, {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            } else {
                def definedFunctions = it.result()
                def undefCount = 0
                def futures = new ArrayList<Future>()
                for (int i = 0; i < definedFunctions.size(); i++) {
                    def qualifiedName = definedFunctions.getJsonObject(i).getString("qualified_name")
                    if (qualifiedName =~ qualifiedNamePattern) {
                        undefCount++
                        def functionId = definedFunctions.getJsonObject(i).getString("function_id")
                        def fut = Future.future()
                        futures.add(fut)
                        refStorage.removeFunctionOwner(functionId, qualifiedName, projectName, fut.completer())
                    }
                }

                CompositeFuture.all(futures).setHandler({
                    if (it.failed()) {
                        it.cause().printStackTrace()
                        System.exit(-1)
                    }

                    println "Undefined $undefCount functions from project $projectName"
                    vertx.close()
                })
            }
        })
    }

}
