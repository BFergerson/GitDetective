package io.gitdetective.web

import com.google.common.base.Charsets
import com.google.common.io.Resources
import grakn.client.Grakn
import grakn.client.GraknClient
import grakn.client.rpc.RPCSession
import graql.lang.Graql
import groovy.util.logging.Slf4j
import io.gitdetective.web.dao.JobsDAO
import io.gitdetective.web.dao.PostgresDAO
import io.gitdetective.web.dao.RedisDAO
import io.gitdetective.web.service.ProjectService
import io.gitdetective.web.service.SystemService
import io.gitdetective.web.service.UserService
import io.gitdetective.web.work.GHArchiveSync
import io.gitdetective.web.work.UpdateFileReferenceCounts
import io.gitdetective.web.work.UpdateFunctionReferenceCounts
import io.gitdetective.web.work.UpdateProjectReferenceCounts
import io.vertx.blueprint.kue.queue.Job
import io.vertx.blueprint.kue.queue.Priority
import io.vertx.blueprint.kue.util.RedisHelper
import io.vertx.core.*
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.bridge.PermittedOptions
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions
import io.vertx.ext.web.handler.sockjs.SockJSHandler

import java.time.Instant
import java.time.temporal.ChronoUnit

import static io.gitdetective.web.WebServices.*

/**
 * todo: description
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
@Slf4j
class GitDetectiveService extends AbstractVerticle {

    private final Router router
    private JobsDAO jobs
    private PostgresDAO postgres
    private GraknClient.Core graknClient
    private ProjectService projectService
    private SystemService systemService
    private UserService userService
    private String uploadsDirectory

    GitDetectiveService(Router router) {
        this.router = router
    }

    @Override
    void start(Promise<Void> startPromise) throws Exception {
        jobs = new JobsDAO(vertx, config())
        uploadsDirectory = config().getString("uploads.directory")
        def redis = new RedisDAO(RedisHelper.client(vertx, config()))
        def postgresFuture = Promise.promise()
        postgres = new PostgresDAO(vertx, config().getJsonObject("storage"), postgresFuture)
        postgresFuture.future().onComplete({
            if (it.succeeded()) {
                //boot/setup grakn
                vertx.executeBlocking({
                    log.info "Grakn integration enabled"
                    String graknHost = config().getString("grakn.host")
                    int graknPort = config().getInteger("grakn.port")
                    String graknKeyspace = config().getString("grakn.keyspace")
                    graknClient = GraknClient.core("$graknHost:$graknPort")
                    setupOntology()

                    //setup services
                    def dataSession = graknClient.session(graknKeyspace, Grakn.Session.Type.DATA)
                    systemService = new SystemService(dataSession, postgres)
                    projectService = new ProjectService(dataSession, postgres)
                    userService = new UserService(dataSession)
                    //todo: async/handler stuff
                    vertx.deployVerticle(systemService)
                    vertx.deployVerticle(projectService)
                    vertx.deployVerticle(userService)
                    vertx.deployVerticle(new UpdateFunctionReferenceCounts(postgres, dataSession), new DeploymentOptions().setWorker(true))
                    vertx.deployVerticle(new UpdateFileReferenceCounts(dataSession), new DeploymentOptions().setWorker(true))
                    vertx.deployVerticle(new UpdateProjectReferenceCounts(dataSession), new DeploymentOptions().setWorker(true))

                    if (config().getBoolean("launch_website")) {
                        log.info "Launching GitDetective website"
                        def options = new DeploymentOptions().setConfig(config())
                        vertx.deployVerticle(new GitDetectiveWebsite(this, router), options)
                        vertx.deployVerticle(new GHArchiveSync(jobs), options)
                    }
                    it.complete()
                }, false, startPromise)
            } else {
                startPromise.fail(it.cause())
            }
        })

        //event bus bridge
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx)
        SockJSBridgeOptions tooltipBridgeOptions = new SockJSBridgeOptions()
                .addInboundPermitted(new PermittedOptions().setAddressRegex(".+"))
                .addOutboundPermitted(new PermittedOptions().setAddressRegex(".+"))
        sockJSHandler.bridge(tooltipBridgeOptions)
        router.route("/backend/services/eventbus/*").handler(sockJSHandler)

        //event bus services
        vertx.eventBus().consumer(GET_PROJECT_REFERENCE_LEADERBOARD, { request ->
            def timer = WebLauncher.metrics.timer(GET_PROJECT_REFERENCE_LEADERBOARD)
            def context = timer.time()
            log.debug "Getting project reference leaderboard"

            int topCount = (request.body() as JsonObject).getInteger("top_count", 5)
            redis.getProjectReferenceLeaderboard(topCount, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                    context.stop()
                    return
                }

                log.debug "Got project reference leaderboard - Size: " + it.result().size()
                request.reply(it.result())
                context.stop()
            })
        })

        //todo: should belong to job service
        vertx.eventBus().consumer(GET_ACTIVE_JOBS, { request ->
            def timer = WebLauncher.metrics.timer(GET_ACTIVE_JOBS)
            def context = timer.time()
            log.debug "Getting active jobs"

            jobs.kue.jobRangeByState("active", 0, Integer.MAX_VALUE, "asc").onComplete({
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                    context.stop()
                    return
                }

                //order by most recently updated
                def finalAllJobs = new ArrayList<Job>(it.result().sort({ it.updated_at }).reverse())
//                //remove jobs with jobs previous to latest
//                it.result().each {
//                    def githubRepository = it.data.getString("github_repository")
//                    if (it.type == GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE) {
//                        finalAllJobs.removeIf({
//                            it.data.getString("github_repository") == githubRepository &&
//                                    it.type != GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE
//                        })
//                    }
//                }

                JsonArray activeJobs = new JsonArray()
                finalAllJobs.each { activeJobs.add(new JsonObject(Json.encode(it))) }
                log.debug "Got active jobs - Size: " + activeJobs.size()
                request.reply(activeJobs)
                context.stop()
            })
        })
        vertx.eventBus().consumer(GET_LATEST_JOB_LOG, { request ->
            def timer = WebLauncher.metrics.timer(GET_LATEST_JOB_LOG)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Getting job log: " + githubRepository

            jobs.getProjectLatestJob(githubRepository, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                    context.stop()
                } else {
                    def job = it.result()
                    if (job.isPresent()) {
                        jobs.getJobLogs(job.get().id, {
                            if (it.failed()) {
                                it.cause().printStackTrace()
                                request.reply(it.cause())
                                context.stop()
                            } else {
                                log.debug "Got job log. Size: " + it.result().size() + " - Repo: " + githubRepository
                                request.reply(new JsonObject().put("job_id", job.get().id).put("logs", it.result()))
                                context.stop()
                            }
                        })
                    } else {
                        log.debug "Found no job logs"
                        request.reply(new JsonObject().put("job_id", -1).put("logs", new JsonArray()))
                        context.stop()
                    }
                }
            })
        })
        vertx.eventBus().consumer(CREATE_JOB, { request ->
            def timer = WebLauncher.metrics.timer(CREATE_JOB)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Creating job"

            // user requested = highest priority
            jobs.createJob("IndexGithubProject", "User build job queued",
                    githubRepository, Priority.CRITICAL, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                    context.stop()
                } else {
                    def jobId = it.result().getId()
                    String result = new JsonObject()
                            .put("message", "User build job queued (id: $jobId)")
                            .put("id", jobId)
                    log.debug "Created job: " + result

                    request.reply(result)
                    context.stop()
                }
            })
        })
        vertx.eventBus().consumer(GET_TRIGGER_INFORMATION, { request ->
            def timer = WebLauncher.metrics.timer(GET_TRIGGER_INFORMATION)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Getting trigger information"

            //check if project in any active jobs
            vertx.eventBus().request(GET_ACTIVE_JOBS, new JsonObject(), {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    context.stop()
                    return
                } else {
                    def activeJobs = it.result().body() as JsonArray
                    for (int i = 0; i < activeJobs.size(); i++) {
                        if (activeJobs.getJsonObject(i).getJsonObject("data")
                                .getString("github_repository") == githubRepository) {
                            log.debug "Found active job for project: $githubRepository"
                            def triggerInformation = new JsonObject()
                            triggerInformation.put("can_queue", false)
                            triggerInformation.put("can_build", false)
                            request.reply(triggerInformation)
                            context.stop()
                            return
                        }
                    }
                }

                //check last queue/build
                def futures = new ArrayList<Future>()
                def canQueueFuture = Promise.promise()
                futures.add(canQueueFuture.future())
                jobs.getProjectLastQueued(githubRepository, canQueueFuture)
                def canBuildFuture = Promise.promise()
                futures.add(canBuildFuture.future())
                redis.getProjectLastBuilt(githubRepository, canBuildFuture)

                CompositeFuture.all(futures).onComplete({
                    if (it.failed()) {
                        it.cause().printStackTrace()
                    } else {
                        def triggerInformation = new JsonObject()
                        triggerInformation.put("can_queue", true)
                        triggerInformation.put("can_build", true)

                        Optional<Instant> lastQueued = it.result().resultAt(0) as Optional<Instant>
                        if (lastQueued.isPresent()) {
                            if (lastQueued.get().plus(24, ChronoUnit.HOURS).isAfter(Instant.now())) {
                                triggerInformation.put("can_queue", false)
                                triggerInformation.put("can_build", false)
                                //todo: remove when things that receive this interpret correctly
                            }
                        }
                        String lastBuilt = it.result().resultAt(1)
                        if (lastBuilt != null) {
                            def lastBuild = Instant.parse(lastBuilt)
                            if (lastBuild.plus(24, ChronoUnit.HOURS).isAfter(Instant.now())) {
                                triggerInformation.put("can_build", false)
                            }
                        }

                        request.reply(triggerInformation)
                    }
                    context.stop()
                })
            })
        })
        log.info "GitDetectiveService started"
    }

    JobsDAO getJobs() {
        return jobs
    }

    PostgresDAO getPostgres() {
        return postgres
    }

    SystemService getSystemService() {
        return systemService
    }

    ProjectService getProjectService() {
        return projectService
    }

    UserService getUserService() {
        return userService
    }

    void setupOntology() {
        log.info "Setting up Grakn ontology"
        def graknKeyspace = config().getString("grakn.keyspace")
        if (!graknClient.databases().contains(graknKeyspace)) {
            graknClient.databases().create(graknKeyspace)
            log.info "Created keyspace: $graknKeyspace"
        }
        try (def schemaSession = graknClient.session(graknKeyspace, Grakn.Session.Type.SCHEMA)) {
            try (def tx = schemaSession.transaction(Grakn.Transaction.Type.WRITE)) {
                tx.query().define(Graql.parseQuery(Resources.toString(Resources.getResource(
                        "gitdetective-schema.gql"), Charsets.UTF_8)))
                tx.commit()
            }
        }
        log.info "Ontology setup"
    }

    static void setupOntology(RPCSession.Core graknSession) {
        log.info "Setting up Grakn ontology"
        try (def tx = graknSession.transaction(Grakn.Transaction.Type.WRITE)) {
            tx.query().define(Graql.parseQuery(Resources.toString(Resources.getResource(
                    "gitdetective-schema.gql"), Charsets.UTF_8)))
            tx.commit()
        }
        log.info "Ontology setup"
    }
}