package io.gitdetective.web

import com.google.common.collect.Lists
import io.gitdetective.GitDetectiveVersion
import io.gitdetective.web.dao.JobsDAO
import io.gitdetective.web.dao.RedisDAO
import io.gitdetective.web.work.calculator.GraknCalculator
import io.gitdetective.web.work.importer.GraknImporter
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.StaticHandler
import io.vertx.ext.web.templ.HandlebarsTemplateEngine

import java.util.concurrent.TimeUnit

import static io.gitdetective.web.Utils.asPrettyNumber
import static io.gitdetective.web.Utils.isValidGithubString
import static io.gitdetective.web.WebServices.*

/**
 * Serves GitDetective website
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GitDetectiveWebsite extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(GitDetectiveWebsite.class)
    private static volatile long CURRENTLY_INDEXING_COUNT = 0
    private static volatile long CURRENTLY_IMPORTING_COUNT = 0
    private static volatile long CURRENTLY_CALCULATING_COUNT = 0
    private static volatile long TOTAL_COMPUTE_TIME = 0
    private static volatile long TOTAL_PROJECT_COUNT = 0
    private static volatile long TOTAL_FILE_COUNT = 0
    private static volatile long TOTAL_METHOD_COUNT = 0
    private static volatile long TOTAL_DEFINITION_COUNT = 0
    private static volatile long TOTAL_REFERENCE_COUNT = 0
    private final JobsDAO jobs
    private final RedisDAO redis
    private final Router router

    GitDetectiveWebsite(JobsDAO jobs, RedisDAO redis, Router router) {
        this.jobs = jobs
        this.redis = redis
        this.router = router
    }

    @Override
    void start() throws Exception {
        //website
        router.route("/static/*").handler(StaticHandler.create()
                .setWebRoot("webroot/static")
                .setCachingEnabled(true))
        router.get("/").handler({ ctx ->
            handleIndex(ctx)
        })
        router.get("/static").handler({ ctx ->
            ctx.response().setStatusCode(404).end()
        })
        router.get("/static/").handler({ ctx ->
            ctx.response().setStatusCode(404).end()
        })
        router.get("/favicon.ico").handler({ ctx ->
            //todo: get favicon
            ctx.response().setStatusCode(404).end()
        })
        router.get("/:githubUsername/:githubProject").handler({ ctx ->
            handleProject(ctx)
        })
        router.get("/:githubUsername/:githubProject/").handler({ ctx ->
            handleProject(ctx)
        })
        router.route().last().handler({
            it.response().putHeader("location", "/")
                    .setStatusCode(302).end()
        })

        //set initial db stats
        updateDatabaseStatistics(true)
        //update every minute
        vertx.setPeriodic(TimeUnit.MINUTES.toMillis(1), {
            updateDatabaseStatistics(false)
            log.info "Updated database statistics"
        })
        log.info "GitDetectiveWebsite started"
    }

    private void updateDatabaseStatistics(boolean initial) {
        jobs.getActiveCount("IndexGithubProject", {
            if (it.succeeded()) {
                CURRENTLY_INDEXING_COUNT = it.result()
            } else {
                it.cause().printStackTrace()
            }
        })
        jobs.getActiveCount(GraknCalculator.GRAKN_CALCULATE_JOB_TYPE, {
            if (it.succeeded()) {
                CURRENTLY_CALCULATING_COUNT = it.result()
            } else {
                it.cause().printStackTrace()
            }
        })
        jobs.getActiveCount(GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE, {
            if (it.succeeded()) {
                CURRENTLY_IMPORTING_COUNT = it.result()
            } else {
                it.cause().printStackTrace()
            }
        })

        if (config().getBoolean("grakn.enabled")) {
            if (initial) {
                redis.getComputeTime({
                    WebLauncher.metrics.counter("GraknComputeTime").inc(TOTAL_COMPUTE_TIME = it.result())
                })
                redis.getProjectCount({
                    WebLauncher.metrics.counter("CreateProject").inc(TOTAL_PROJECT_COUNT = it.result())
                })
                redis.getFileCount({
                    WebLauncher.metrics.counter("ImportFile").inc(TOTAL_FILE_COUNT = it.result())
                })
                redis.getMethodCount({
                    WebLauncher.metrics.counter("ImportMethod").inc(TOTAL_METHOD_COUNT = it.result())
                })
                redis.getDefinitionCount({
                    WebLauncher.metrics.counter("ImportDefinedFunction").inc(TOTAL_DEFINITION_COUNT = it.result())
                })
                redis.getReferenceCount({
                    WebLauncher.metrics.counter("ImportReferencedFunction").inc(TOTAL_REFERENCE_COUNT = it.result())
                })
            } else {
                redis.cacheComputeTime(TOTAL_COMPUTE_TIME = WebLauncher.metrics.counter("GraknComputeTime").getCount())
                redis.cacheProjectCount(TOTAL_PROJECT_COUNT = WebLauncher.metrics.counter("CreateProject").getCount())
                redis.cacheFileCount(TOTAL_FILE_COUNT = WebLauncher.metrics.counter("ImportFile").getCount())
                redis.cacheMethodCount(TOTAL_METHOD_COUNT = WebLauncher.metrics.counter("ImportMethod").getCount())
                redis.cacheDefinitionCount(TOTAL_DEFINITION_COUNT = WebLauncher.metrics.counter("ImportDefinedFunction").getCount())
                redis.cacheReferenceCount(TOTAL_REFERENCE_COUNT = WebLauncher.metrics.counter("ImportReferencedFunction").getCount())
            }
        } else {
            redis.getComputeTime({ TOTAL_COMPUTE_TIME = it.result() })
            redis.getProjectCount({ TOTAL_PROJECT_COUNT = it.result() })
            redis.getFileCount({ TOTAL_FILE_COUNT = it.result() })
            redis.getMethodCount({ TOTAL_METHOD_COUNT = it.result() })
            redis.getDefinitionCount({ TOTAL_DEFINITION_COUNT = it.result() })
            redis.getReferenceCount({ TOTAL_REFERENCE_COUNT = it.result() })
        }
    }

    private void handleIndex(RoutingContext ctx) {
        ctx.put("gitdetective_url", config().getString("gitdetective_url"))
        ctx.put("gitdetective_eventbus_url", config().getString("gitdetective_url") + "backend/services/eventbus")
        ctx.put("gitdetective_version", GitDetectiveVersion.version)

        //load and send page data
        CompositeFuture.all(Lists.asList(
                getActiveJobs(ctx),
                getProjectReferenceLeaderboard(ctx),
                getDatabaseStatistics(ctx)
        )).setHandler({
            HandlebarsTemplateEngine engine = HandlebarsTemplateEngine.create()
            engine.render(ctx, "webroot/index.hbs", { res ->
                if (res.succeeded()) {
                    ctx.response().end(res.result())
                } else {
                    ctx.fail(res.cause())
                }
            })
        })
    }

    private Future getActiveJobs(RoutingContext ctx) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().send(GET_ACTIVE_JOBS, new JsonObject(), {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                //add pretty job type
                def activeJobs = it.result().body() as JsonArray
                for (int i = 0; i < activeJobs.size(); i++) {
                    def job = activeJobs.getJsonObject(i)
                    if (job.getString("type") == GraknCalculator.GRAKN_CALCULATE_JOB_TYPE) {
                        job.getJsonObject("data").put("job_type", "Calculating")
                    } else if (job.getString("type") == GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE) {
                        job.getJsonObject("data").put("job_type", "Importing")
                    } else {
                        job.getJsonObject("data").put("job_type", "Indexing")
                    }
                }

                ctx.put("active_jobs", it.result().body())
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectReferenceLeaderboard(RoutingContext ctx) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().send(GET_PROJECT_REFERENCE_LEADERBOARD, new JsonObject(), {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                def referenceLeaderboard = it.result().body() as JsonArray

                //make counts pretty
                for (int i = 0; i < referenceLeaderboard.size(); i++) {
                    def project = referenceLeaderboard.getJsonObject(i)
                    def count = project.getString("value") as int
                    project.put("value", asPrettyNumber(count))
                }
                ctx.put("project_reference_leaderboard", referenceLeaderboard)
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private void handleProject(RoutingContext ctx) {
        def username = ctx.pathParam("githubUsername")
        def project = ctx.pathParam("githubProject")
        if (!isValidGithubString(username) || !isValidGithubString(project)) {
            //invalid github username/project
            ctx.response().putHeader("location", "/")
                    .setStatusCode(302).end()
            return
        } else {
            ctx.put("github_username", username)
            ctx.put("github_project", project)
            ctx.put("github_repository", "$username/$project")
        }
        ctx.put("gitdetective_url", config().getString("gitdetective_url"))
        ctx.put("gitdetective_eventbus_url", config().getString("gitdetective_url") + "backend/services/eventbus")
        ctx.put("gitdetective_version", GitDetectiveVersion.version)

        //load and send page data
        def githubRepo = new JsonObject().put("github_repo", "$username/$project")
        CompositeFuture.all(Lists.asList(
                getLatestBuildLog(ctx, githubRepo),
                getProjectFileCount(ctx, githubRepo),
                getProjectMethodVersionCount(ctx, githubRepo),
                getProjectFirstIndexed(ctx, githubRepo),
                getProjectLastIndexed(ctx, githubRepo),
                getProjectLastIndexedCommitInformation(ctx, githubRepo),
                getProjectLastCalculated(ctx, githubRepo),
                getProjectMostReferencedMethods(ctx, githubRepo)
        )).setHandler({
            HandlebarsTemplateEngine engine = HandlebarsTemplateEngine.create()
            engine.render(ctx, "webroot/project.hbs", { res ->
                if (res.succeeded()) {
                    ctx.response().end(res.result())
                } else {
                    ctx.fail(res.cause())
                }
            })
        })

        //schedule build/recalculate if can
        vertx.eventBus().send(GET_TRIGGER_INFORMATION, githubRepo, {
            def triggerInformation = it.result().body() as JsonObject
            if (triggerInformation.getBoolean("can_build")) {
                log.info "Auto-building: " + githubRepo.getString("github_repo")
                vertx.eventBus().send(CREATE_JOB, githubRepo)
            } else if (triggerInformation.getBoolean("can_recalculate")) {
                log.info "Auto-recalculating: " + githubRepo.getString("github_repo")
                vertx.eventBus().send(TRIGGER_RECALCULATION, githubRepo)
            }
        })
    }

    private Future getLatestBuildLog(RoutingContext ctx, JsonObject githubRepo) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().send(GET_LATEST_JOB_LOG, githubRepo, {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                def jobLog = it.result().body() as JsonObject
                ctx.put("latest_job_log", jobLog.getJsonArray("logs"))
                ctx.put("latest_job_log_id", jobLog.getLong("job_id"))
                ctx.put("latest_job_log_position", jobLog.getJsonArray("logs").size() - 1)
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectFileCount(RoutingContext ctx, JsonObject githubRepo) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().send(GET_PROJECT_FILE_COUNT, githubRepo, {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                ctx.put("project_file_count", it.result().body())
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectMethodVersionCount(RoutingContext ctx, JsonObject githubRepo) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().send(GET_PROJECT_METHOD_INSTANCE_COUNT, githubRepo, {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                ctx.put("project_method_version_count", it.result().body())
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectMostReferencedMethods(RoutingContext ctx, JsonObject githubRepo) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().send(GET_PROJECT_MOST_REFERENCED_METHODS, githubRepo, {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                ctx.put("project_most_referenced_methods", it.result().body())
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectFirstIndexed(RoutingContext ctx, JsonObject githubRepo) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().send(GET_PROJECT_FIRST_INDEXED, githubRepo, {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                ctx.put("project_first_indexed", it.result().body())
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectLastIndexed(RoutingContext ctx, JsonObject githubRepo) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().send(GET_PROJECT_LAST_INDEXED, githubRepo, {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                ctx.put("project_last_indexed", it.result().body())
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectLastIndexedCommitInformation(RoutingContext ctx, JsonObject githubRepo) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().send(GET_PROJECT_LAST_INDEXED_COMMIT_INFORMATION, githubRepo, {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                def commitInformation = it.result().body() as JsonObject
                if (commitInformation != null) {
                    commitInformation.put("commit_short", commitInformation.getString("commit").substring(0, 7))
                    ctx.put("project_last_indexed_commit_information", commitInformation)
                }
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectLastCalculated(RoutingContext ctx, JsonObject githubRepo) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().send(GET_PROJECT_LAST_CALCULATED, githubRepo, {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                ctx.put("project_last_calculated", it.result().body())
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private static Future getDatabaseStatistics(RoutingContext ctx) {
        def stats = new JsonArray()
        stats.add(new JsonObject().put("stat1", "Active backlog").put("value1",
                asPrettyNumber(CURRENTLY_INDEXING_COUNT + CURRENTLY_IMPORTING_COUNT + CURRENTLY_CALCULATING_COUNT))
                .put("stat2", "Projects").put("value2", asPrettyNumber(TOTAL_PROJECT_COUNT)))
        stats.add(new JsonObject().put("stat1", "Definitions").put("value1", asPrettyNumber(TOTAL_DEFINITION_COUNT))
                .put("stat2", "Files").put("value2", asPrettyNumber(TOTAL_FILE_COUNT)))
        stats.add(new JsonObject().put("stat1", "References").put("value1", asPrettyNumber(TOTAL_REFERENCE_COUNT))
                .put("stat2", "Methods").put("value2", asPrettyNumber(TOTAL_METHOD_COUNT)))
        ctx.put("database_statistics", stats)

        def future = Future.future()
        def handler = future.completer()
        handler.handle(Future.succeededFuture())
        return future
    }

}
