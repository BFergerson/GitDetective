package io.gitdetective.web

import ai.grakn.Grakn
import ai.grakn.GraknConfigKey
import ai.grakn.GraknTxType
import ai.grakn.Keyspace
import ai.grakn.engine.GraknConfig
import com.google.common.base.Charsets
import com.google.common.io.Resources
import io.gitdetective.web.dao.GraknDAO
import io.gitdetective.web.dao.JobsDAO
import io.gitdetective.web.dao.RedisDAO
import io.gitdetective.web.work.GHArchiveSync
import io.gitdetective.web.work.calculator.GraknCalculator
import io.gitdetective.web.work.importer.GraknImporter
import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.Job
import io.vertx.blueprint.kue.queue.Priority
import io.vertx.blueprint.kue.util.RedisHelper
import io.vertx.core.*
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.sockjs.BridgeOptions
import io.vertx.ext.web.handler.sockjs.PermittedOptions
import io.vertx.ext.web.handler.sockjs.SockJSHandler
import javassist.ClassPool
import javassist.CtClass
import javassist.CtMethod
import org.apache.commons.lang.SystemUtils
import org.joor.Reflect

import java.time.Instant
import java.time.temporal.ChronoUnit

import static io.gitdetective.web.WebServices.*
import static java.util.UUID.randomUUID

/**
 * todo: description
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GitDetectiveService extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(GitDetectiveService.class)
    private final Router router
    private final Kue kue
    private String uploadsDirectory

    GitDetectiveService(Router router, Kue kue) {
        this.router = router
        this.kue = kue
    }

    @Override
    void start() {
        uploadsDirectory = config().getString("uploads.directory")
        def redis = new RedisDAO(RedisHelper.client(vertx, config()))
        def jobs = new JobsDAO(kue, redis)

        vertx.executeBlocking({
            def importJobEnabled = config().getJsonObject("importer").getBoolean("enabled")
            def calculateJobEnabled = config().getJsonObject("calculator").getBoolean("enabled")
            if (config().getBoolean("grakn.enabled")) {
                log.info "Grakn integration enabled"
                setupOntology()

                if (importJobEnabled) {
                    def grakn = makeGraknDAO(redis)
                    log.info "Import job processing enabled"
                    def importerOptions = new DeploymentOptions().setConfig(config())
                    vertx.deployVerticle(new GraknImporter(kue, redis, grakn, uploadsDirectory), importerOptions)
                } else {
                    log.info "Import job processing disabled"
                }
                if (calculateJobEnabled) {
                    def grakn = makeGraknDAO(redis)
                    log.info "Reference calculation job processing enabled"
                    def calculatorOptions = new DeploymentOptions().setConfig(config())
                    vertx.deployVerticle(new GraknCalculator(kue, redis, grakn), calculatorOptions)
                } else {
                    log.info "Reference calculation job processing disabled"
                }
            } else if (importJobEnabled || calculateJobEnabled) {
                log.error "Job processing cannot be enabled with Grakn disabled"
                System.exit(-1)
            } else {
                log.info "Grakn integration disabled"
            }
            it.complete()

            if (config().getBoolean("launch_website")) {
                log.info "Launching GitDetective website"
                def options = new DeploymentOptions().setConfig(config())
                vertx.deployVerticle(new GitDetectiveWebsite(jobs, redis, router), options)
                vertx.deployVerticle(new GHArchiveSync(jobs, redis), options)
            }
        }, false, {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }
        })

        //routes
        router.post("/indexes").handler(this.&downloadIndexFile)
        router.post("/jobs/transfer").handler(this.&transferJob)

        //event bus bridge
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx)
        BridgeOptions tooltipBridgeOptions = new BridgeOptions()
                .addInboundPermitted(new PermittedOptions().setAddressRegex(".+"))
                .addOutboundPermitted(new PermittedOptions().setAddressRegex(".+"))
        sockJSHandler.bridge(tooltipBridgeOptions)
        router.route("/backend/services/eventbus/*").handler(sockJSHandler)

        //event bus services
        vertx.eventBus().consumer(GET_PROJECT_REFERENCE_LEADERBOARD, { request ->
            def timer = WebLauncher.metrics.timer(GET_PROJECT_REFERENCE_LEADERBOARD)
            def context = timer.time()
            log.debug "Getting project reference leaderboard"

            redis.getProjectReferenceLeaderboard(10, {
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
        vertx.eventBus().consumer(GET_ACTIVE_JOBS, { request ->
            def timer = WebLauncher.metrics.timer(GET_ACTIVE_JOBS)
            def context = timer.time()
            log.debug "Getting active jobs"

            //get all active jobs; return most recent 10
            kue.jobRangeByState("active", 0, Integer.MAX_VALUE, "asc").setHandler({
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                    context.stop()
                    return
                }
                def allJobs = it.result()

                //order by most recently updated
                allJobs = allJobs.sort({ it.updated_at }).reverse()
                //remove jobs with jobs previous to latest
                def finalAllJobs = new ArrayList<Job>(allJobs)
                allJobs.each {
                    def githubRepository = it.data.getString("github_repository")
                    if (it.type == GraknCalculator.GRAKN_CALCULATE_JOB_TYPE) {
                        finalAllJobs.removeIf({
                            it.data.getString("github_repository") == githubRepository &&
                                    it.type != GraknCalculator.GRAKN_CALCULATE_JOB_TYPE
                        })
                    } else if (it.type == GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE) {
                        finalAllJobs.removeIf({
                            it.data.getString("github_repository") == githubRepository &&
                                    it.type != GraknCalculator.GRAKN_CALCULATE_JOB_TYPE &&
                                    it.type != GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE
                        })
                    }
                }

                JsonArray activeJobs = new JsonArray()
                //encode jobs
                finalAllJobs.each { activeJobs.add(new JsonObject(Json.encode(it))) }
                //only most recent 10
                if (activeJobs.size() > 10) {
                    activeJobs = new JsonArray(activeJobs.take(10))
                }

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
        vertx.eventBus().consumer(TRIGGER_RECALCULATION, { request ->
            def timer = WebLauncher.metrics.timer(TRIGGER_RECALCULATION)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()

            //check if can re-calculator
            vertx.eventBus().send(GET_TRIGGER_INFORMATION, new JsonObject().put("github_repository", githubRepository), {
                def triggerInformation = it.result().body() as JsonObject
                if (triggerInformation.getBoolean("can_recalculate")) {
                    log.debug "Triggering recalculation"

                    // user requested = highest priority
                    jobs.createJob(GraknCalculator.GRAKN_CALCULATE_JOB_TYPE,
                            "User reference recalculation queued",
                            new JsonObject().put("github_repository", githubRepository)
                                    .put("is_recalculation", true)
                                    .put("build_skipped", true),
                            Priority.CRITICAL, { job ->
                        if (job.failed()) {
                            job.cause().printStackTrace()
                            request.reply(job.cause())
                            context.stop()
                        } else {
                            def jobId = job.result().getId()
                            String result = new JsonObject()
                                    .put("message", "User reference recalculation queued (id: $jobId)")
                                    .put("id", jobId)
                            log.debug "Created recalculation job: " + result
                            request.reply(result)
                            context.stop()
                        }
                    })
                }
            })
        })
        vertx.eventBus().consumer(GET_PROJECT_FILE_COUNT, { request ->
            def timer = WebLauncher.metrics.timer(GET_PROJECT_FILE_COUNT)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Getting project file count: " + githubRepository

            redis.getProjectFileCount(githubRepository, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    log.debug "Got file count: " + it.result() + " - Repo: " + githubRepository
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer(GET_PROJECT_METHOD_INSTANCE_COUNT, { request ->
            def timer = WebLauncher.metrics.timer(GET_PROJECT_METHOD_INSTANCE_COUNT)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Getting project method instance count: " + githubRepository

            redis.getProjectMethodInstanceCount(githubRepository, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    log.debug "Got method instance count: " + it.result() + " - Repo: " + githubRepository
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer(GET_PROJECT_MOST_REFERENCED_METHODS, { request ->
            def timer = WebLauncher.metrics.timer(GET_PROJECT_MOST_REFERENCED_METHODS)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Getting project most referenced methods"

            redis.getProjectMostExternalReferencedMethods(githubRepository, 10, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    def methodMap = new HashMap<String, JsonObject>()
                    for (int i = 0; i < it.result().size(); i++) {
                        def array = it.result()
                        for (int z = 0; z < array.size(); z++) {
                            def method = array.getJsonObject(z)
                            if (methodMap.containsKey(method.getString("id"))) {
                                methodMap.get(method.getString("id")).mergeIn(method)
                            } else {
                                methodMap.put(method.getString("id"), method)
                            }
                        }
                    }

                    //sort methods by external reference count
                    def result = new JsonArray(methodMap.values().asList().sort {
                        return it.getInteger("external_reference_count")
                    }.reverse())

                    //method link
                    for (int i = 0; i < result.size(); i++) {
                        def ob = result.getJsonObject(i)
                        if (ob.getInteger("external_reference_count") > 0) {
                            ob.put("has_method_link", true)
                        } else {
                            break
                        }
                    }

                    log.debug "Got project most referenced methods - Size: " + result.size()
                    request.reply(result)
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer(GET_METHOD_EXTERNAL_REFERENCES, { request ->
            def timer = WebLauncher.metrics.timer(GET_METHOD_EXTERNAL_REFERENCES)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            def methodId = body.getString("method_id")
            def offset = body.getInteger("offset")
            log.debug "Getting method external references"

            redis.getMethodExternalReferences(githubRepository, methodId, offset, 10, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    log.debug "Got method external references: " + it.result()
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer(GET_PROJECT_FIRST_INDEXED, { request ->
            def timer = WebLauncher.metrics.timer(GET_PROJECT_FIRST_INDEXED)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Getting project first indexed"

            redis.getProjectFirstIndexed(githubRepository, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    log.debug "Got project first indexed: " + it.result()
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer(GET_PROJECT_LAST_INDEXED, { request ->
            def timer = WebLauncher.metrics.timer(GET_PROJECT_LAST_INDEXED)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Getting project last indexed"

            redis.getProjectLastIndexed(githubRepository, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    log.debug "Got project last indexed: " + it.result()
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer(GET_PROJECT_LAST_INDEXED_COMMIT_INFORMATION, { request ->
            def timer = WebLauncher.metrics.timer(GET_PROJECT_LAST_INDEXED_COMMIT_INFORMATION)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Getting project last indexed commit information"

            redis.getProjectLastIndexedCommitInformation(githubRepository, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    log.debug "Got project last indexed commit information: " + it.result()
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer(GET_PROJECT_LAST_CALCULATED, { request ->
            def timer = WebLauncher.metrics.timer(GET_PROJECT_LAST_CALCULATED)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Getting project last calculated"

            redis.getProjectLastCalculated(githubRepository, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    log.debug "Got project last calculated: " + it.result()
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer(GET_TRIGGER_INFORMATION, { request ->
            def timer = WebLauncher.metrics.timer(GET_TRIGGER_INFORMATION)
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepository = body.getString("github_repository").toLowerCase()
            log.debug "Getting trigger information"

            def futures = new ArrayList<Future>()
            def canQueueFuture = Future.future()
            futures.add(canQueueFuture)
            jobs.getProjectLastQueued(githubRepository, canQueueFuture.completer())
            def canBuildFuture = Future.future()
            futures.add(canBuildFuture)
            redis.getProjectLastBuilt(githubRepository, canBuildFuture.completer())
            def canRecalculateFuture = Future.future()
            futures.add(canRecalculateFuture)
            redis.getProjectLastCalculated(githubRepository, canRecalculateFuture.completer())

            CompositeFuture.all(futures).setHandler({
                if (it.failed()) {
                    it.cause().printStackTrace()
                } else {
                    def calcConfig = config().getJsonObject("calculator")
                    def triggerInformation = new JsonObject()
                    triggerInformation.put("can_queue", true)
                    triggerInformation.put("can_build", true)
                    triggerInformation.put("can_recalculate", true)

                    Optional<Instant> lastQueued = it.result().resultAt(0) as Optional<Instant>
                    if (lastQueued.isPresent()) {
                        if (lastQueued.get().plus(24, ChronoUnit.HOURS).isAfter(Instant.now())) {
                            triggerInformation.put("can_queue", false)
                            triggerInformation.put("can_build", false)
                            //todo: remove when things that receive this interpret correctly
                        }
                        if (lastQueued.get().plus(calcConfig.getInteger("project_recalculate_wait_time"),
                                ChronoUnit.HOURS).isAfter(Instant.now())) {
                            triggerInformation.put("can_recalculate", false)
                            //todo: remove when things that receive this interpret correctly
                        }
                    } else {
                        //no queue = no re-calc
                        triggerInformation.put("can_recalculate", false)
                    }
                    String lastBuilt = it.result().resultAt(1)
                    if (lastBuilt != null) {
                        def lastBuild = Instant.parse(lastBuilt)
                        if (lastBuild.plus(24, ChronoUnit.HOURS).isAfter(Instant.now())) {
                            triggerInformation.put("can_build", false)
                        }
                    } else {
                        //no build = no re-calc
                        triggerInformation.put("can_recalculate", false)
                    }
                    String lastCalculated = it.result().resultAt(2)
                    if (lastCalculated != null) {
                        def lastCalculation = Instant.parse(lastCalculated)
                        if (lastCalculation.plus(calcConfig.getInteger("project_recalculate_wait_time"),
                                ChronoUnit.HOURS).isAfter(Instant.now())) {
                            triggerInformation.put("can_recalculate", false)
                        }
                    } else {
                        //no initial calc = no re-calc
                        triggerInformation.put("can_recalculate", false)
                    }

                    request.reply(triggerInformation)
                }
                context.stop()
            })
        })
        log.info "GitDetectiveService started"
    }

    private GraknDAO makeGraknDAO(RedisDAO redis) {
        String graknHost = config().getString("grakn.host")
        int graknPort = config().getInteger("grakn.port")
        String graknKeyspace = config().getString("grakn.keyspace")
        def keyspace = Keyspace.of(graknKeyspace)
        def session = Grakn.session(graknHost + ":" + graknPort, keyspace)
        if (SystemUtils.IS_OS_WINDOWS) {
            //start of hacks because Grakn doesn't make things easy for Windows :/
            try {
                GraknConfig config = Reflect.on(session).get("config")
                config.setConfigProperty(GraknConfigKey.STORAGE_HOSTNAME, "192.168.99.100")

                CtClass clazz = ClassPool.getDefault().get("org.apache.cassandra.thrift.EndpointDetails")
                CtMethod originalMethod = clazz.getDeclaredMethod("getHost")
                originalMethod.setBody("return \"" + graknHost + "\";")
                clazz.toClass()
            } catch (Exception e) {
                e.printStackTrace()
            }
            //end of hacks because Grakn didn't make things easy for Windows :/
        }
        return new GraknDAO(vertx, redis, session)
    }

    private void setupOntology() {
        log.info "Setting up Grakn ontology"
        String graknHost = config().getString("grakn.host")
        int graknPort = config().getInteger("grakn.port")
        String graknKeyspace = config().getString("grakn.keyspace")
        def keyspace = Keyspace.of(graknKeyspace)
        def session = Grakn.session(graknHost + ":" + graknPort, keyspace)
        if (SystemUtils.IS_OS_WINDOWS) {
            //start of hacks because Grakn doesn't make things easy for Windows :/
            try {
                GraknConfig config = Reflect.on(session).get("config")
                config.setConfigProperty(GraknConfigKey.STORAGE_HOSTNAME, "192.168.99.100")

                CtClass clazz = ClassPool.getDefault().get("org.apache.cassandra.thrift.EndpointDetails")
                CtMethod originalMethod = clazz.getDeclaredMethod("getHost")
                originalMethod.setBody("return \"" + graknHost + "\";")
                clazz.toClass()
            } catch (Exception e) {
                e.printStackTrace()
            }
            //end of hacks because Grakn didn't make things easy for Windows :/
        }

        def tx = session.open(GraknTxType.WRITE)
        def graql = tx.graql()
        def query = graql.parse(Resources.toString(Resources.getResource("gitdetective-schema.gql"), Charsets.UTF_8))
        query.execute()
        tx.commit()
        session.close()
        log.info "Ontology setup"
    }

    private void downloadIndexFile(RoutingContext routingContext) {
        def uuid = randomUUID() as String
        log.info "Downloading index file. Index id: " + uuid

        def downloadFile = new File(uploadsDirectory, uuid + ".zip")
        downloadFile.parentFile.mkdirs()
        downloadFile.createNewFile()
        vertx.fileSystem().writeFile(downloadFile.absolutePath, routingContext.body,
                new Handler<AsyncResult<Void>>() {
                    @Override
                    void handle(AsyncResult<Void> result) {
                        if (result.failed()) {
                            result.cause().printStackTrace()
                        }
                    }
                })
        routingContext.response().end(uuid)
    }

    private void transferJob(RoutingContext routingContext) {
        def jobData = routingContext.bodyAsJson
        if (jobData instanceof LinkedHashMap) {
            jobData = JsonObject.mapFrom(jobData)
        }
        Job job = new Job(jobData)
        jobData.put("github_repository", job.data.getString("github_repository"))

        if (job.data.getBoolean("build_skipped")) {
            kue.createJob(GraknCalculator.GRAKN_CALCULATE_JOB_TYPE, jobData)
                    .setMax_attempts(0)
                    .setPriority(job.priority)
                    .save().setHandler({
                if (it.failed()) {
                    it.cause().printStackTrace()
                } else {
                    logPrintln(job, "Calculator received job")
                }
            })
        } else {
            kue.createJob(GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE, jobData)
                    .setMax_attempts(0)
                    .setRemoveOnComplete(true)
                    .setPriority(job.priority)
                    .save().setHandler({
                if (it.failed()) {
                    it.cause().printStackTrace()
                } else {
                    logPrintln(job, "Importer received job")
                }
            })
        }
        routingContext.response().end()
    }

}