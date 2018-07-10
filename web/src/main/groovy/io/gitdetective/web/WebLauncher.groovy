package io.gitdetective.web

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.CsvReporter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.SharedMetricRegistries
import io.gitdetective.GitDetectiveVersion
import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.Job
import io.vertx.blueprint.kue.queue.KueVerticle
import io.vertx.blueprint.kue.util.RedisHelper
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonObject
import io.vertx.core.net.JksOptions
import io.vertx.ext.dropwizard.DropwizardMetricsOptions
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.redis.RedisClient
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import static io.gitdetective.web.Utils.messageCodec

/**
 * Web main entry
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class WebLauncher {

    public static final MetricRegistry metrics = new MetricRegistry()

    static void main(String[] args) {
        println "GitDetective Web - Version: " + GitDetectiveVersion.version
        File configInputStream = new File("web-config.json")
        String configData = IOUtils.toString(new FileInputStream(configInputStream), StandardCharsets.UTF_8)
        DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject(configData))
        def serviceConfig = options.config.getJsonObject("service")

        def vertxOptions = new VertxOptions()
        vertxOptions.maxWorkerExecuteTime = TimeUnit.MINUTES.toNanos(serviceConfig.getInteger("max_worker_time_minutes"))
        vertxOptions.workerPoolSize = serviceConfig.getInteger("worker_pool_size")
        vertxOptions.internalBlockingPoolSize = serviceConfig.getInteger("blocking_pool_size")

        File file = new File("vertx-metrics")
        file.mkdirs()
        vertxOptions.metricsOptions = new DropwizardMetricsOptions().setEnabled(true).setRegistryName("vertx-metrics")
        MetricRegistry registry = SharedMetricRegistries.getOrCreate("vertx-metrics")
        def reporter = CsvReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build(file)
        reporter.start(1, TimeUnit.MINUTES)

        def vertx = Vertx.vertx(vertxOptions)
        vertx.eventBus().registerDefaultCodec(Job.class, messageCodec(Job.class))
        Router router = Router.router(vertx)
        router.route().handler(BodyHandler.create())
        if (options.config.getBoolean("ssl_enabled")) {
            if (!new File("server-keystore.jks").exists()) {
                System.err.println("Keystore file required to run with SSL enabled")
                System.exit(-1)
            }
            vertx.createHttpServer(new HttpServerOptions().setSsl(options.config.getBoolean("ssl_enabled"))
                    .setKeyStoreOptions(new JksOptions().setPath("server-keystore.jks")
                    .setPassword(options.config.getString("keystore_password"))
            )).requestHandler(router.&accept).listen(443, {
                if (it.failed()) {
                    if (it.cause() instanceof BindException) {
                        System.err.println("Failed to bind to port: 443")
                    } else {
                        it.cause().printStackTrace()
                    }
                    System.exit(-1)
                } else {
                    println "GitDetective active on port: 443"
                }
            })
            addHttpRedirection(vertx, options.config)
        } else {
            vertx.createHttpServer().requestHandler(router.&accept).listen(80, {
                if (it.failed()) {
                    if (it.cause() instanceof BindException) {
                        System.err.println("Failed to bind to port: 80")
                    } else {
                        it.cause().printStackTrace()
                    }
                    System.exit(-1)
                } else {
                    println "GitDetective active on port: 80"
                }
            })
        }

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

        println "Launching GitDetective service"
        vertx.deployVerticle(new KueVerticle(), kueOptions)
        Kue kue = new Kue(vertx, kueOptions.config)
        vertx.deployVerticle(new GitDetectiveService(router, kue, redisClient), options, {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }
        })

        reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build()
        reporter.start(10, TimeUnit.MINUTES)
    }

    private static void addHttpRedirection(Vertx vertx, JsonObject config) {
        Router redirectRouter = Router.router(vertx)
        redirectRouter.route().handler({
            def redirectLocation = config.getString("gitdetective_url")
            if (!redirectLocation.endsWith("/")) {
                redirectLocation += "/"
            }
            if (it.request().path() != "/") {
                if (it.request().path().startsWith("/")) {
                    redirectLocation += it.request().path().substring(1)
                } else {
                    redirectLocation += it.request().path()
                }
            }
            it.response().putHeader("location", redirectLocation).setStatusCode(302).end()
        })
        vertx.createHttpServer().requestHandler(redirectRouter.&accept).listen(80, {
            if (it.failed()) {
                if (it.cause() instanceof BindException) {
                    System.err.println("Failed to bind to port: 80")
                } else {
                    it.cause().printStackTrace()
                }
                System.exit(-1)
            } else {
                println "GitDetective active on port: 80"
            }
        })
    }

}
