package io.gitdetective.indexer

import com.codahale.metrics.MetricRegistry
import groovy.util.logging.Slf4j
import io.gitdetective.indexer.extractor.MavenReferenceExtractor
import io.gitdetective.indexer.stage.GithubRepositoryCloner
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
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets

import static io.gitdetective.indexer.IndexerServices.messageCodec

/**
 * Indexer main entry
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
@Slf4j
class GitDetectiveIndexer extends AbstractVerticle {

    public static final MetricRegistry metrics = new MetricRegistry()
    private final static ResourceBundle buildBundle = ResourceBundle.getBundle("gitdetective_build")

    static void main(String[] args) {
        log.info "GitDetective Indexer - Version: " + buildBundle.getString("version")
        System.setProperty("vertx.disableFileCPResolving", "true")
        def configInputStream = new File("indexer-config.json").newInputStream()
        def config = new JsonObject(IOUtils.toString(configInputStream, StandardCharsets.UTF_8))
        def deployOptions = new DeploymentOptions().setConfig(config)
        def vertxOptions = new VertxOptions()
        vertxOptions.maxWorkerExecuteTime = Long.MAX_VALUE
        def vertx = Vertx.vertx(vertxOptions)

        def kueOptions = new DeploymentOptions().setConfig(config)
        if (config.getJsonObject("jobs_server") != null) {
            kueOptions.config = config.getJsonObject("jobs_server")
        }

        def kue = new Kue(vertx, kueOptions.config)
        vertx.deployVerticle(new KueVerticle(kue), kueOptions, {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }

            vertx.deployVerticle(new GitDetectiveIndexer(kue), deployOptions, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    System.exit(-1)
                }
            })
        })
    }

    private final Kue kue

    GitDetectiveIndexer(Kue kue) {
        this.kue = kue
    }

    @Override
    void start() throws Exception {
        vertx.eventBus().registerDefaultCodec(Job.class, messageCodec(Job.class))
        def redis = new RedisDAO(RedisHelper.client(vertx, config()))
        def jobs = new JobsDAO(vertx, config())
        def deployOptions = new DeploymentOptions()
        deployOptions.config = config()

        //core
        vertx.deployVerticle(new GithubRepositoryCloner(kue, jobs), deployOptions)

        //extractors
        vertx.deployVerticle(new MavenReferenceExtractor(), deployOptions)
    }
}
