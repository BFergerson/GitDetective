package io.gitdetective.web

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.CsvReporter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.SharedMetricRegistries
import io.gitdetective.GitDetectiveVersion
import io.gitdetective.web.dao.JobsDAO
import io.gitdetective.web.dao.RedisDAO
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
        def configInputStream = new File("web-config.json").newInputStream()
        def config = new JsonObject(IOUtils.toString(configInputStream, StandardCharsets.UTF_8))
        def deployOptions = new DeploymentOptions().setConfig(config)
        def serviceConfig = deployOptions.config.getJsonObject("service")
        def vertxOptions = new VertxOptions()
        vertxOptions.maxWorkerExecuteTime = TimeUnit.MINUTES.toNanos(serviceConfig.getInteger("max_worker_time_minutes"))
        vertxOptions.workerPoolSize = serviceConfig.getInteger("worker_pool_size")
        vertxOptions.internalBlockingPoolSize = serviceConfig.getInteger("blocking_pool_size")

        def vertx = Vertx.vertx(vertxOptions)
        vertx.eventBus().registerDefaultCodec(Job.class, messageCodec(Job.class))
        Router router = Router.router(vertx)
        router.route().handler(BodyHandler.create())
        if (deployOptions.config.getBoolean("ssl_enabled")) {
            if (!new File("server-keystore.jks").exists()) {
                System.err.println("Keystore file required to run with SSL enabled")
                System.exit(-1)
            }
            vertx.createHttpServer(new HttpServerOptions().setSsl(deployOptions.config.getBoolean("ssl_enabled"))
                    .setKeyStoreOptions(new JksOptions().setPath("server-keystore.jks")
                    .setPassword(deployOptions.config.getString("keystore_password"))
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
            addHttpRedirection(vertx, deployOptions.config)
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

        def kueOptions = new DeploymentOptions().setConfig(config)
        if (deployOptions.config.getJsonObject("jobs_server") != null) {
            kueOptions.config = deployOptions.config.getJsonObject("jobs_server")
        }

        println "Launching GitDetective service"
        vertx.deployVerticle(new KueVerticle(), kueOptions, {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }

            def kue = new Kue(vertx, kueOptions.config)
            def jobs = new JobsDAO(kue, new RedisDAO(RedisHelper.client(vertx, kueOptions.config)))
            vertx.deployVerticle(new GitDetectiveService(router, kue, jobs), deployOptions, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    System.exit(-1)
                }
            })
        })

        setupMetricReporters(vertxOptions)
    }

    private static void setupMetricReporters(VertxOptions vertxOptions) {
        File file = new File("vertx-metrics")
        file.mkdirs()
        vertxOptions.metricsOptions = new DropwizardMetricsOptions().setEnabled(true).setRegistryName("vertx-metrics")
        MetricRegistry registry = SharedMetricRegistries.getOrCreate("vertx-metrics")
        def reporter = CsvReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build(file)
        reporter.start(1, TimeUnit.MINUTES)

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
