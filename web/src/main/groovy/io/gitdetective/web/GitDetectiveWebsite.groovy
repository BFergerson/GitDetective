package io.gitdetective.web

import com.google.common.collect.Lists
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.StaticHandler
import io.vertx.ext.web.templ.handlebars.HandlebarsTemplateEngine

import javax.net.ssl.SSLException
import java.nio.channels.ClosedChannelException
import java.util.concurrent.TimeUnit

import static io.gitdetective.web.WebServices.*

/**
 * Serves GitDetective website
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GitDetectiveWebsite extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(GitDetectiveWebsite.class)
    private final static ResourceBundle buildBundle = ResourceBundle.getBundle("gitdetective_build")
    private static volatile long CURRENTLY_INDEXING_COUNT = 0
    private static volatile long CURRENTLY_IMPORTING_COUNT = 0
    private static volatile long TOTAL_COMPUTE_TIME = 0
    private static volatile long TOTAL_PROJECT_COUNT = 0
    private static volatile long TOTAL_FILE_COUNT = 0
    private static volatile long TOTAL_FUNCTION_COUNT = 0
    private static volatile long TOTAL_DEFINITION_COUNT = 0
    private static volatile long TOTAL_REFERENCE_COUNT = 0
    private final GitDetectiveService service
    private final Router router
    private HandlebarsTemplateEngine engine

    GitDetectiveWebsite(GitDetectiveService service, Router router) {
        this.service = service
        this.router = router
    }

    @Override
    void start() throws Exception {
        //website
        engine = HandlebarsTemplateEngine.create(vertx)
        router.route("/static/*").handler(StaticHandler.create()
                .setWebRoot("webroot/static")
                .setCachingEnabled(true))
        router.get("/helper/*").handler(StaticHandler.create()
                .setWebRoot("webroot/helper")
                .setCachingEnabled(true))
        router.getWithRegex(".*js.map").handler({ ctx ->
            ctx.response().setStatusCode(404).end()
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
        router.get("/").handler({ ctx ->
            handleIndexPage(ctx)
        })
        router.get("/projects/leaderboard").handler({ ctx ->
            handleProjectLeaderboardPage(ctx)
        })
        router.get("/functions/leaderboard").handler({ ctx ->
            handleFunctionLeaderboardPage(ctx)
        })
        router.get("/:githubUsername").handler({ ctx ->
            handleUserPage(ctx)
        })
        router.get("/:githubUsername/").handler({ ctx ->
            handleUserPage(ctx)
        })
        router.get("/:githubUsername/:githubProject").handler({ ctx ->
            handleProjectPage(ctx)
        })
        router.get("/:githubUsername/:githubProject/").handler({ ctx ->
            handleProjectPage(ctx)
        })
        router.route().last().handler({
            it.response().putHeader("location", "/")
                    .setStatusCode(302).end()
        })
        router.route().failureHandler({
            if ((it.failure() instanceof IllegalStateException && it.failure().message == "Response is closed")
                    || (it.failure() instanceof SSLException && it.failure().message == "SSLEngine closed already")
                    || it.failure() instanceof ClosedChannelException) {
                log.warn it.failure().message //todo: why do these happen?
            } else if (it.failure() instanceof IOException && it.failure().message == "Broken pipe") {
                log.warn it.failure().message //todo: why do these happen?
            } else {
                it.failure().printStackTrace()
            }
        })

        //set initial db stats
        updateDatabaseStatistics()
        //update periodically
        vertx.setPeriodic(TimeUnit.MINUTES.toMillis(15), {
            updateDatabaseStatistics()
            log.info "Updated database statistics"
        })
        log.info "GitDetectiveWebsite started"
    }

    private void updateDatabaseStatistics() {
//        jobs.getActiveCount("IndexGithubProject", {
//            if (it.succeeded()) {
//                CURRENTLY_INDEXING_COUNT = it.result()
//            } else {
//                it.cause().printStackTrace()
//            }
//        })
//        jobs.getActiveCount(GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE, {
//            if (it.succeeded()) {
//                CURRENTLY_IMPORTING_COUNT = it.result()
//            } else {
//                it.cause().printStackTrace()
//            }
//        })

//            redis.getComputeTime({
//                WebLauncher.metrics.counter("GraknComputeTime").inc(TOTAL_COMPUTE_TIME = it.result())
//            })
        service.systemService.getTotalProjectCount({
            TOTAL_PROJECT_COUNT = it.result()
        })
        service.systemService.getTotalFileCount({
            TOTAL_FILE_COUNT = it.result()
        })
        service.systemService.getTotalFunctionCount({
            TOTAL_FUNCTION_COUNT = it.result()
        })
//            service.systemService.getDefinitionCount({
//                WebLauncher.metrics.counter("ImportDefinedFunction").inc(TOTAL_DEFINITION_COUNT = it.result())
//            })
//            service.systemService.getReferenceCount({
//                WebLauncher.metrics.counter("ImportReferencedFunction").inc(TOTAL_REFERENCE_COUNT = it.result())
//            })
    }

    private void handleIndexPage(RoutingContext ctx) {
        ctx.put("gitdetective_url", config().getString("gitdetective_url"))
        ctx.put("gitdetective_static_url", config().getString("gitdetective_static_url"))
        ctx.put("gitdetective_eventbus_url", config().getString("gitdetective_url") + "backend/services/eventbus")
        ctx.put("gitdetective_version", buildBundle.getString("version"))

        //load and send page data
        log.debug "Loading index page"
        CompositeFuture.all(Lists.asList(
                getActiveJobs(ctx),
                getProjectReferenceLeaderboard(ctx, 5),
                getFunctionReferenceLeaderboard(ctx, 5),
                getDatabaseStatistics(ctx)
        )).setHandler({
            log.debug "Rendering index page"
            engine.render(ctx.data(), "webroot/index.hbs", { res ->
                if (res.succeeded()) {
                    log.info "Displaying index page"
                    ctx.response().end(res.result())
                } else {
                    ctx.fail(res.cause())
                }
            })
        })
    }

    private void handleUserPage(RoutingContext ctx) {
        def username = ctx.pathParam("githubUsername")

        if (!isValidGithubString(username)) {
            //invalid github username
            ctx.response().putHeader("location", "/")
                    .setStatusCode(302).end()
            return
        } else {
            ctx.put("github_username", username)
        }
        ctx.put("gitdetective_url", config().getString("gitdetective_url"))
        ctx.put("gitdetective_static_url", config().getString("gitdetective_static_url"))
        ctx.put("gitdetective_eventbus_url", config().getString("gitdetective_url") + "backend/services/eventbus")
        ctx.put("gitdetective_version", buildBundle.getString("version"))

        //load and send page data
        log.debug "Loading user page: $username"
        CompositeFuture.all(Lists.asList(
                getUserProjectCount(ctx, username),
                getUserMostReferencedProjectsInformation(ctx, username)
        )).setHandler({
            log.debug "Rendering user page: $username"
            engine.render(ctx.data(), "webroot/user.hbs", { res ->
                if (res.succeeded()) {
                    log.info "Displaying user page: $username"
                    ctx.response().end(res.result())
                } else {
                    ctx.fail(res.cause())
                }
            })
        })
    }

    private void handleProjectPage(RoutingContext ctx) {
        def username = ctx.pathParam("githubUsername")
        def project = ctx.pathParam("githubProject")
        def githubRepository = "$username/$project"

        if (!isValidGithubString(username) || !isValidGithubString(project)) {
            //invalid github username/project
            ctx.response().putHeader("location", "/")
                    .setStatusCode(302).end()
            return
        } else {
            ctx.put("github_username", username)
            ctx.put("github_project", project)
            ctx.put("github_repository", githubRepository)
        }
        ctx.put("gitdetective_url", config().getString("gitdetective_url"))
        ctx.put("gitdetective_static_url", config().getString("gitdetective_static_url"))
        ctx.put("gitdetective_eventbus_url", config().getString("gitdetective_url") + "backend/services/eventbus")
        ctx.put("gitdetective_version", buildBundle.getString("version"))

        //load and send page data
        log.debug "Loading project page: $username/$project"
        def repo = new JsonObject().put("github_repository", "$username/$project")
        CompositeFuture.all(Lists.asList(
                getLatestBuildLog(ctx, repo),
                getProjectFileCount(ctx, githubRepository),
                getProjectFunctionCount(ctx, githubRepository),
                getProjectReferenceTrend(ctx, githubRepository),
//                getProjectFirstIndexed(ctx, repo),
//                getProjectLastIndexed(ctx, repo),
//                getProjectLastIndexedCommitInformation(ctx, repo),
                getProjectMostReferencedFunctionsInformation(ctx, githubRepository)
        )).setHandler({
            if (it.failed()) {
                it.cause().printStackTrace()
            }

            log.debug "Rendering project page: $username/$project"
            engine.render(ctx.data(), "webroot/project.hbs", { res ->
                if (res.succeeded()) {
                    log.info "Displaying project page: $username/$project"
                    ctx.response().end(res.result())
                } else {
                    ctx.fail(res.cause())
                }
            })
        })

//        if (config().getBoolean("auto_build_enabled")) {
//            //schedule build/recalculate if can
//            def autoBuilt = autoBuildCache.getIfPresent(githubRepository)
//            if (autoBuilt == null) {
//                log.debug "Checking repository: $githubRepository"
//                autoBuildCache.put(githubRepository, true)
//
//                vertx.eventBus().send(GET_TRIGGER_INFORMATION, repo, {
//                    def triggerInformation = it.result().body() as JsonObject
//                    if (triggerInformation.getBoolean("can_build")) {
//                        log.info "Auto-building: " + repo.getString("github_repository")
//                        vertx.eventBus().send(CREATE_JOB, repo)
//                    }
//                })
//            }
//        }
    }

    private void handleProjectLeaderboardPage(RoutingContext ctx) {
        ctx.put("gitdetective_url", config().getString("gitdetective_url"))
        ctx.put("gitdetective_static_url", config().getString("gitdetective_static_url"))
        ctx.put("gitdetective_eventbus_url", config().getString("gitdetective_url") + "backend/services/eventbus")
        ctx.put("gitdetective_version", buildBundle.getString("version"))

        //load and send page data
        log.debug "Loading project leaderboard page"
        getProjectReferenceLeaderboard(ctx, 100).setHandler({
            if (it.failed()) {
                ctx.fail(it.cause())
            } else {
                log.debug "Rendering project leaderboard page"
                engine.render(ctx.data(), "webroot/project_leaderboard.hbs", { res ->
                    if (res.succeeded()) {
                        log.info "Displaying project leaderboard page"
                        ctx.response().end(res.result())
                    } else {
                        ctx.fail(res.cause())
                    }
                })
            }
        })
    }

    private void handleFunctionLeaderboardPage(RoutingContext ctx) {
        ctx.put("gitdetective_url", config().getString("gitdetective_url"))
        ctx.put("gitdetective_static_url", config().getString("gitdetective_static_url"))
        ctx.put("gitdetective_eventbus_url", config().getString("gitdetective_url") + "backend/services/eventbus")
        ctx.put("gitdetective_version", buildBundle.getString("version"))

        //load and send page data
        log.debug "Loading function leaderboard page"
        getFunctionReferenceLeaderboard(ctx, 100).setHandler({
            if (it.failed()) {
                ctx.fail(it.cause())
            } else {
                log.debug "Rendering function leaderboard page"
                engine.render(ctx.data(), "webroot/function_leaderboard.hbs", { res ->
                    if (res.succeeded()) {
                        log.info "Displaying function leaderboard page"
                        ctx.response().end(res.result())
                    } else {
                        ctx.fail(res.cause())
                    }
                })
            }
        })
    }

    private Future getActiveJobs(RoutingContext ctx) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().request(GET_ACTIVE_JOBS, new JsonObject(), {
            if (it.failed()) {
                ctx.fail(it.cause())
            } else {
                def activeJobs = it.result().body() as JsonArray
                //only most recent 10
                if (activeJobs.size() > 10) {
                    activeJobs = new JsonArray(activeJobs.take(10))
                }
                //add pretty job type
                for (int i = 0; i < activeJobs.size(); i++) {
                    def job = activeJobs.getJsonObject(i)
//                    if (job.getString("type") == GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE) {
//                        job.getJsonObject("data").put("job_type", "Importing")
//                    } else {
                    job.getJsonObject("data").put("job_type", "Indexing")
//                    }
                }

                ctx.put("active_jobs", activeJobs)
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectReferenceLeaderboard(RoutingContext ctx, int limit) {
        def future = Future.future()
        def handler = future.completer()
        service.systemService.getTotalMostReferencedProjectsInformation(limit, {
            if (it.failed()) {
                ctx.fail(it.cause())
            } else {
                def totalProjectReferences = new JsonArray()
                it.result().each {
                    def reference = new JsonObject()
                    reference.put("project_name", it.projectName)
                    reference.put("github_repository", it.usernameAndProjectName)
                    reference.put("external_reference_count", asPrettyNumber(it.referenceCount))
                    totalProjectReferences.add(reference)
                }
                ctx.put("project_reference_leaderboard", totalProjectReferences)
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getFunctionReferenceLeaderboard(RoutingContext ctx, int limit) {
        def future = Future.future()
        def handler = future.completer()
        service.systemService.getTotalMostReferencedFunctionsInformation(limit, {
            if (it.failed()) {
                ctx.fail(it.cause())
            } else {
                ctx.put("function_reference_leaderboard", new JsonArray(Json.encode(it.result())))
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getLatestBuildLog(RoutingContext ctx, JsonObject githubRepository) {
        def future = Future.future()
        def handler = future.completer()
        vertx.eventBus().request(GET_LATEST_JOB_LOG, githubRepository, {
            if (it.failed()) {
                ctx.fail(it.cause())
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

    private Future getUserProjectCount(RoutingContext ctx, String githubUsername) {
        def future = Future.future()
        def handler = future.completer()
        service.userService.getProjectCount("github:" + githubUsername, {
            if (it.failed()) {
                ctx.fail(it.cause())
            } else {
                ctx.put("user_project_count", it.result())
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getUserMostReferencedProjectsInformation(RoutingContext ctx, String githubUsername) {
        def future = Future.future()
        def handler = future.completer()
        service.userService.getMostReferencedProjectsInformation("github:" + githubUsername, 10, {
            if (it.failed()) {
                ctx.fail(it.cause())
            } else {
                def userProjectReferences = new JsonArray()
                it.result().each {
                    def reference = new JsonObject()
                    reference.put("project_name", it.projectName)
                    reference.put("simple_project_name", it.simpleProjectName)
                    reference.put("external_reference_count", it.referenceCount)
                    userProjectReferences.add(reference)
                }
                ctx.put("user_most_referenced_projects", userProjectReferences)
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectReferenceTrend(RoutingContext ctx, String githubRepository) {
        def future = Future.future()
        def handler = future.completer()
        service.projectService.getProjectId("github:" + githubRepository, {
            if (it.succeeded()) {
                if (it.result() != null) {
                    service.postgres.getProjectReferenceTrend(it.result(), {
                        if (it.failed()) {
                            ctx.fail(it.cause())
                        } else {
                            def cumulativeTrendData = new JsonObject()
                            cumulativeTrendData.put("trend_available", !it.result().trendData.isEmpty())
                            def trendData = new JsonArray()
                            cumulativeTrendData.put("trend_data", trendData)

                            def referenceCount = 0
                            it.result().trendData.each {
                                referenceCount += it.count

                                def data = new JsonObject()
                                data.put("time", it.time)
                                data.put("count", referenceCount)
                                trendData.add(data)
                            }
                            ctx.put("project_reference_trend", cumulativeTrendData)
                        }
                        handler.handle(Future.succeededFuture())
                    })
                } else {
                    handler.handle(Future.succeededFuture())
                }
            } else {
                ctx.fail(it.cause())
            }
        })
        return future
    }

    private Future getProjectFileCount(RoutingContext ctx, String githubRepository) {
        def future = Future.future()
        def handler = future.completer()
        service.projectService.getFileCount("github:" + githubRepository, {
            if (it.failed()) {
                ctx.fail(it.cause())
            } else {
                ctx.put("project_file_count", it.result())
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectFunctionCount(RoutingContext ctx, String githubRepository) {
        def future = Future.future()
        def handler = future.completer()
        service.projectService.getFunctionCount("github:" + githubRepository, {
            if (it.failed()) {
                ctx.fail(it.cause())
            } else {
                ctx.put("project_function_count", it.result())
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

    private Future getProjectMostReferencedFunctionsInformation(RoutingContext ctx, String githubRepository) {
        def future = Future.future()
        def handler = future.completer()
        service.projectService.getMostReferencedFunctionsInformation("github:" + githubRepository, 10, {
            if (it.failed()) {
                ctx.fail(it.cause())
            } else {
                def projectFunctionReferences = new JsonArray()
                it.result().each {
                    def reference = new JsonObject()
                    reference.put("has_method_link", it.referenceCount > 0)
                    reference.put("id", it.functionId)
                    reference.put("class_name", it.className)
                    reference.put("short_class_name", it.shortClassName)
                    reference.put("method_signature", it.functionSignature)
                    reference.put("short_method_signature", it.shortFunctionSignature)
                    reference.put("external_reference_count", it.referenceCount)
                    projectFunctionReferences.add(reference)
                }
                ctx.put("project_most_referenced_functions", projectFunctionReferences)
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

//    private Future getProjectFileCount(RoutingContext ctx, JsonObject githubRepository) {
//        def future = Future.future()
//        def handler = future.completer()
//        vertx.eventBus().send(GET_PROJECT_FILE_COUNT, githubRepository, {
//            if (it.failed()) {
//                ctx.fail(it.cause())
//            } else {
//                ctx.put("project_file_count", it.result().body())
//            }
//            handler.handle(Future.succeededFuture())
//        })
//        return future
//    }
//
//    private Future getProjectMethodVersionCount(RoutingContext ctx, JsonObject githubRepository) {
//        def future = Future.future()
//        def handler = future.completer()
//        vertx.eventBus().send(GET_PROJECT_METHOD_INSTANCE_COUNT, githubRepository, {
//            if (it.failed()) {
//                ctx.fail(it.cause())
//            } else {
//                ctx.put("project_method_version_count", it.result().body())
//            }
//            handler.handle(Future.succeededFuture())
//        })
//        return future
//    }
//
//    private Future getProjectFirstIndexed(RoutingContext ctx, JsonObject githubRepository) {
//        def future = Future.future()
//        def handler = future.completer()
//        vertx.eventBus().send(GET_PROJECT_FIRST_INDEXED, githubRepository, {
//            if (it.failed()) {
//                ctx.fail(it.cause())
//            } else {
//                ctx.put("project_first_indexed", it.result().body())
//            }
//            handler.handle(Future.succeededFuture())
//        })
//        return future
//    }
//
//    private Future getProjectLastIndexed(RoutingContext ctx, JsonObject githubRepository) {
//        def future = Future.future()
//        def handler = future.completer()
//        vertx.eventBus().send(GET_PROJECT_LAST_INDEXED, githubRepository, {
//            if (it.failed()) {
//                ctx.fail(it.cause())
//            } else {
//                ctx.put("project_last_indexed", it.result().body())
//            }
//            handler.handle(Future.succeededFuture())
//        })
//        return future
//    }
//
//    private Future getProjectLastIndexedCommitInformation(RoutingContext ctx, JsonObject githubRepository) {
//        def future = Future.future()
//        def handler = future.completer()
//        vertx.eventBus().send(GET_PROJECT_LAST_INDEXED_COMMIT_INFORMATION, githubRepository, {
//            if (it.failed()) {
//                ctx.fail(it.cause())
//            } else {
//                def commitInformation = it.result().body() as JsonObject
//                if (commitInformation != null) {
//                    commitInformation.put("commit_short", commitInformation.getString("commit").substring(0, 7))
//                    ctx.put("project_last_indexed_commit_information", commitInformation)
//                }
//            }
//            handler.handle(Future.succeededFuture())
//        })
//        return future
//    }

    private static Future getDatabaseStatistics(RoutingContext ctx) {
        def stats = new JsonArray()
        stats.add(new JsonObject().put("stat1", "Active backlog").put("value1",
                asPrettyNumber(CURRENTLY_INDEXING_COUNT + CURRENTLY_IMPORTING_COUNT))
                .put("stat2", "Projects").put("value2", asPrettyNumber(TOTAL_PROJECT_COUNT)))
        stats.add(new JsonObject().put("stat1", "Definitions").put("value1", asPrettyNumber(TOTAL_DEFINITION_COUNT))
                .put("stat2", "Files").put("value2", asPrettyNumber(TOTAL_FILE_COUNT)))
        stats.add(new JsonObject().put("stat1", "References").put("value1", asPrettyNumber(TOTAL_REFERENCE_COUNT))
                .put("stat2", "Functions").put("value2", asPrettyNumber(TOTAL_FUNCTION_COUNT)))
        ctx.put("database_statistics", stats)

        def future = Future.future()
        def handler = future.completer()
        handler.handle(Future.succeededFuture())
        return future
    }
}
