package io.gitdetective.web

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.google.common.collect.Lists
import io.gitdetective.web.model.FunctionInformation
import io.gitdetective.web.service.ProjectService
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.auth.PubSecKeyOptions
import io.vertx.ext.auth.jwt.JWTAuth
import io.vertx.ext.auth.jwt.JWTAuthOptions
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.JWTAuthHandler
import io.vertx.ext.web.handler.StaticHandler
import io.vertx.ext.web.templ.handlebars.HandlebarsTemplateEngine

import javax.net.ssl.SSLException
import java.nio.channels.ClosedChannelException
import java.util.concurrent.TimeUnit

import static io.gitdetective.web.WebServices.*
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE
import static java.util.Objects.requireNonNull

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
    private static volatile long TOTAL_PROJECT_COUNT = 0
    private static volatile long TOTAL_FILE_COUNT = 0
    private static volatile long TOTAL_FUNCTION_COUNT = 0
    private static volatile long TOTAL_UNIQUE_REFERENCE_COUNT = 0
    private static volatile long TOTAL_REFERENCE_COUNT = 0
    private static final Cache<String, Boolean> autoBuildCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES).build()
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
                .setWebRoot(config().getString("webroot_location") + "/static")
                .setCachingEnabled(true))
        router.get("/helper/*").handler(StaticHandler.create()
                .setWebRoot(config().getString("webroot_location") + "/helper")
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
            ctx.response().setStatusCode(404).end() //todo: get favicon
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

        //api
        def provider = JWTAuth.create(vertx, new JWTAuthOptions()
                .addPubSecKey(new PubSecKeyOptions()
                        .setAlgorithm("HS256")
                        .setPublicKey(config().getString("api_key"))
                        .setSymmetric(true)))
        def v1ApiRouter = Router.router(vertx)
        router.mountSubRouter("/api", v1ApiRouter)
        v1ApiRouter.route("/*").handler(JWTAuthHandler.create(provider))
        v1ApiRouter.post("/users/:githubUsername").handler({ ctx ->
            service.userService.getOrCreateUser("github:" + ctx.pathParam("githubUsername"), {
                if (it.succeeded()) {
                    ctx.response().setStatusCode(200).end(it.result())
                } else {
                    it.cause().printStackTrace()
                    ctx.response().setStatusCode(500).end(it.cause().message)
                }
            })
        })
        v1ApiRouter.post("/users/:githubUsername/projects/:githubProject").handler({ ctx ->
            def githubUsername = "github:" + ctx.pathParam("githubUsername")
            service.userService.getOrCreateUser(githubUsername, {
                if (it.succeeded()) {
                    service.projectService.getOrCreateProject(it.result(),
                            githubUsername + "/" + ctx.pathParam("githubProject"), {
                        if (it.succeeded()) {
                            ctx.response().setStatusCode(200).end(it.result())
                        } else {
                            it.cause().printStackTrace()
                            ctx.response().setStatusCode(500).end(it.cause().message)
                        }
                    })
                } else {
                    it.cause().printStackTrace()
                    ctx.response().setStatusCode(500).end(it.cause().message)
                }
            })
        })
        v1ApiRouter.post("/files").handler({ ctx ->
            log.info("Insert files request")
            def request = new JsonArray()
            def requestStr = ctx.getBodyAsString()
            if (requestStr.startsWith("[")) {
                request = new JsonArray(requestStr)
            } else {
                request.add(new JsonObject(requestStr))
            }

            def files = new ArrayList<Tuple3<String, String, String>>()
            for (int i = 0; i < request.size(); i++) {
                def req = request.getJsonObject(i)
                def projectId = requireNonNull(req.getString("project_id"))
                def qualifiedName = requireNonNull(req.getString("qualified_name"))
                def fileLocation = requireNonNull(req.getString("file_location"))
                files << Tuple.tuple(projectId, qualifiedName, fileLocation)
            }
            service.projectService.getOrCreateFiles(files, {
                if (it.succeeded()) {
                    log.info("Insert files request succeeded")
                    ctx.response().headers().add(CONTENT_TYPE, "application/json")
                    ctx.response().setStatusCode(200).end(Json.encode(it.result()))
                } else {
                    log.error("Insert files request failed", it.cause())
                    ctx.response().setStatusCode(500).end(it.cause().message)
                }
            })
        })
        v1ApiRouter.post("/functions").handler({ ctx ->
            log.info("Insert functions request")
            def request = new JsonArray()
            def requestStr = ctx.getBodyAsString()
            if (requestStr.startsWith("[")) {
                request = new JsonArray(requestStr)
            } else {
                request.add(new JsonObject(requestStr))
            }

            vertx.executeBlocking({ blocking ->
                def results = new ArrayList<String>()
                def writeTx = service.graknSession.transaction().write()
                for (int i = 0; i < request.size(); i++) {
                    def req = request.getJsonObject(i)
                    def fileId = req.getString("file_id")
                    def kytheUri = requireNonNull(req.getString("kythe_uri"))
                    def qualifiedName = requireNonNull(req.getString("qualified_name"))
                    def functionId = ProjectService.getOrCreateFunction(writeTx, fileId,
                            new FunctionInformation(kytheUri, qualifiedName), fileId == null ? 1 : 0)
                    results << functionId
                }
                writeTx.commit()
                blocking.complete(results)
            }, false, {
                if (it.succeeded()) {
                    log.info("Insert functions request succeeded")
                    ctx.response().setStatusCode(200).end(Json.encode(it.result()))
                } else {
                    log.error("Insert functions request failed", it.cause())
                    ctx.response().setStatusCode(500).end(it.cause().message)
                }
            })
        })
        v1ApiRouter.post("/references").handler({ ctx ->
            log.info("Insert references request")
            def request = new JsonArray()
            def requestStr = ctx.getBodyAsString()
            if (requestStr.startsWith("[")) {
                request = new JsonArray(requestStr)
            } else {
                request.add(new JsonObject(requestStr))
            }

            vertx.executeBlocking({ blocking ->
                def futures = new ArrayList<Future>()
                def writeTx = service.graknSession.transaction().write()
                for (int i = 0; i < request.size(); i++) {
                    def req = request.getJsonObject(i)
                    def projectId = requireNonNull(req.getString("project_id"))
                    def callerCommitDate = requireNonNull(req.getInstant("caller_commit_date"))
                    def callerCommitSha1 = requireNonNull(req.getString("caller_commit_sha1"))
                    def callerLineNumber = req.getInteger("caller_line_number")
                    def callerFunctionId = requireNonNull(req.getString("caller_function_id"))
                    def calleeFunctionId = requireNonNull(req.getString("callee_function_id"))
                    ProjectService.getOrCreateFunctionReference(writeTx, callerFunctionId, calleeFunctionId)

                    def future = Future.future()
                    futures << future
                    service.postgres.insertFunctionReference(projectId, calleeFunctionId, calleeFunctionId,
                            callerCommitSha1, callerCommitDate, callerLineNumber, future)
                }
                writeTx.commit()

                CompositeFuture.all(futures).setHandler({
                    if (it.succeeded()) {
                        blocking.complete()
                    } else {
                        blocking.fail(it.cause())
                    }
                })
            }, false, {
                if (it.succeeded()) {
                    log.info("Insert references request succeeded")
                    ctx.response().setStatusCode(200).end()
                } else {
                    log.error("Insert references request failed", it.cause())
                    ctx.response().setStatusCode(500).end(it.cause().message)
                }
            })
        })

        //general
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
        service.jobs.getActiveCount("IndexGithubProject", {
            if (it.succeeded()) {
                CURRENTLY_INDEXING_COUNT = it.result()
            } else {
                it.cause().printStackTrace()
            }
        })
        service.jobs.getActiveCount("ImportGithubProject", {
            if (it.succeeded()) {
                CURRENTLY_IMPORTING_COUNT = it.result()
            } else {
                it.cause().printStackTrace()
            }
        })
        service.systemService.getTotalUniqueReferenceCount({
            if (it.succeeded()) {
                TOTAL_UNIQUE_REFERENCE_COUNT = it.result()
            } else {
                it.cause().printStackTrace()
            }
        })
        service.systemService.getTotalReferenceCount({
            if (it.succeeded()) {
                TOTAL_REFERENCE_COUNT = it.result()
            } else {
                it.cause().printStackTrace()
            }
        })
        service.systemService.getTotalProjectCount({
            if (it.succeeded()) {
                TOTAL_PROJECT_COUNT = it.result()
            } else {
                it.cause().printStackTrace()
            }
        })
        service.systemService.getTotalFileCount({
            if (it.succeeded()) {
                TOTAL_FILE_COUNT = it.result()
            } else {
                it.cause().printStackTrace()
            }
        })
        service.systemService.getTotalFunctionCount({
            if (it.succeeded()) {
                TOTAL_FUNCTION_COUNT = it.result()
            } else {
                it.cause().printStackTrace()
            }
        })
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
                getLiveProjectReferenceTrend(ctx, githubRepository),
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

        if (config().getBoolean("auto_build_enabled")) {
            //schedule build/recalculate if can
            def autoBuilt = autoBuildCache.getIfPresent(githubRepository)
            if (autoBuilt == null) {
                log.debug "Checking repository: $githubRepository"
                autoBuildCache.put(githubRepository, true)

                vertx.eventBus().send(GET_TRIGGER_INFORMATION, repo, {
                    def triggerInformation = it.result().body() as JsonObject
                    if (triggerInformation.getBoolean("can_build")) {
                        log.info "Auto-building: " + repo.getString("github_repository")
                        vertx.eventBus().send(CREATE_JOB, repo)
                    }
                })
            }
        }
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
                    activeJobs = new JsonArray(activeJobs.take(10).toList())
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
        service.projectService.getFunctionIds("github:" + githubRepository, {
            if (it.succeeded()) {
                service.postgres.getProjectReferenceTrend(it.result(), {
                    if (it.succeeded()) {
                        def cumulativeTrendData = new JsonObject()
                        cumulativeTrendData.put("trend_available", it.result().trendData.size() > 1)
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
                        handler.handle(Future.succeededFuture())
                    } else {
                        ctx.fail(it.cause())
                    }
                })
            } else {
                ctx.fail(it.cause())
            }
        })
        return future
    }

    private Future getLiveProjectReferenceTrend(RoutingContext ctx, String githubRepository) {
        def future = Future.future()
        def handler = future.completer()
        service.projectService.getFunctionIds("github:" + githubRepository, {
            if (it.succeeded()) {
                service.postgres.getLiveProjectReferenceTrend(it.result(), {
                    if (it.succeeded()) {
                        def cumulativeTrendData = new JsonObject()
                        cumulativeTrendData.put("live_trend_available", it.result().trendData.size() > 1)
                        def trendData = new JsonArray()
                        cumulativeTrendData.put("live_trend_data", trendData)

                        def referenceCount = 0
                        it.result().trendData.each {
                            referenceCount += it.count

                            def data = new JsonObject()
                            data.put("time", it.time)
                            data.put("count", referenceCount)
                            trendData.add(data)
                        }
                        ctx.put("live_project_reference_trend", cumulativeTrendData)
                        handler.handle(Future.succeededFuture())
                    } else {
                        ctx.fail(it.cause())
                    }
                })
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
                    reference.put("has_function_link", it.referenceCount > 0)
                    reference.put("id", it.functionId)
                    reference.put("class_name", it.className)
                    reference.put("short_class_name", it.shortClassName)
                    reference.put("function_signature", it.functionSignature)
                    reference.put("short_function_signature", it.shortFunctionSignature)
                    reference.put("external_reference_count", it.referenceCount)
                    projectFunctionReferences.add(reference)
                }
                ctx.put("project_most_referenced_functions", projectFunctionReferences)
            }
            handler.handle(Future.succeededFuture())
        })
        return future
    }

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
        stats.add(new JsonObject().put("stat1", "Unique References").put("value1", asPrettyNumber(TOTAL_UNIQUE_REFERENCE_COUNT))
                .put("stat2", "Files").put("value2", asPrettyNumber(TOTAL_FILE_COUNT)))
        stats.add(new JsonObject().put("stat1", "Total References").put("value1", asPrettyNumber(TOTAL_REFERENCE_COUNT))
                .put("stat2", "Functions").put("value2", asPrettyNumber(TOTAL_FUNCTION_COUNT)))
        ctx.put("database_statistics", stats)

        def future = Future.future()
        def handler = future.completer()
        handler.handle(Future.succeededFuture())
        return future
    }
}
