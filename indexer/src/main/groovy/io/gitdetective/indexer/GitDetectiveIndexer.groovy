package io.gitdetective.indexer

import com.codahale.metrics.MetricRegistry
import io.gitdetective.GitDetectiveVersion
import io.gitdetective.indexer.stage.*
import io.gitdetective.indexer.support.KytheMavenBuilder
import io.gitdetective.web.dao.JobsDAO
import io.gitdetective.web.dao.RedisDAO
import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.Job
import io.vertx.blueprint.kue.queue.KueVerticle
import io.vertx.blueprint.kue.util.RedisHelper
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.JsonObject
import io.vertx.redis.RedisClient
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets

import static io.gitdetective.web.Utils.messageCodec

/**
 * Indexer main entry
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GitDetectiveIndexer extends AbstractVerticle {

    public static final MetricRegistry metrics = new MetricRegistry()

    static void main(String[] args) {
        println "GitDetective Indexer - Version: " + GitDetectiveVersion.version
        System.setProperty("vertx.disableFileCPResolving", "true")

        def configInputStream = new File("indexer-config.json").newInputStream()
        def configData = IOUtils.toString(configInputStream, StandardCharsets.UTF_8)
        def options = new DeploymentOptions().setConfig(new JsonObject(configData))

        def vertxOptions = new VertxOptions()
        vertxOptions.maxWorkerExecuteTime = Long.MAX_VALUE
        def vertx = Vertx.vertx(vertxOptions)

        def cacheCluster = options.config.getJsonArray("cache_cluster")
        def kueOptions = new DeploymentOptions()
        RedisClient redisClient
        if (cacheCluster.size() == 1) {
            println "Using cache in standalone mode"
            def serverConfig = cacheCluster.getJsonObject(0)
            redisClient = RedisHelper.client(vertx, serverConfig)
            kueOptions.setConfig(serverConfig)
        } else {
            throw new IllegalStateException("Not yet implemented")
        }
        if (options.config.getJsonObject("jobs_server") != null) {
            kueOptions.config = options.config.getJsonObject("jobs_server")
        }

        def kue = new Kue(vertx, kueOptions.config)
        vertx.deployVerticle(new KueVerticle(), kueOptions, {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }
            vertx.deployVerticle(new GitDetectiveIndexer(kue, redisClient), options, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    System.exit(-1)
                }

                //start processing
                vertx.eventBus().send(GithubRepositoryCloner.PROCESS_NEXT_JOB, new JsonObject())
            })
        })
    }

    private final Kue kue
    private final RedisClient redisClient

    GitDetectiveIndexer(Kue kue, RedisClient redisClient) {
        this.kue = kue
        this.redisClient = redisClient
    }

    @Override
    void start() throws Exception {
        vertx.eventBus().registerDefaultCodec(Job.class, messageCodec(Job.class))
        def redis = new RedisDAO(redisClient)
        def jobs = new JobsDAO(kue, redis)
        def deployOptions = new DeploymentOptions()
        deployOptions.config = config()

        //core
        vertx.deployVerticle(new GithubArchiveSync(jobs, redis), deployOptions)
        vertx.deployVerticle(new GithubRepositoryCloner(kue, jobs, redis), deployOptions)
        vertx.deployVerticle(new KytheIndexOutput(), deployOptions)
        vertx.deployVerticle(new KytheUsageExtractor(), deployOptions)
        vertx.deployVerticle(new GitDetectiveImportFilter(redis), deployOptions)
        vertx.deployVerticle(new KytheIndexAugment(redis), deployOptions)

        //project builders
        vertx.deployVerticle(new KytheMavenBuilder(), deployOptions)
    }

}
