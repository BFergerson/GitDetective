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

import static io.gitdetective.web.Utils.logPrintln
import static java.util.UUID.randomUUID

/**
 * todo: description
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GitDetectiveService extends AbstractVerticle {

    private final Router router
    private final Kue kue
    private final JobsDAO jobs
    private String uploadsDirectory

    GitDetectiveService(Router router, Kue kue, JobsDAO jobs) {
        this.router = router
        this.kue = kue
        this.jobs = jobs
    }

    @Override
    void start() {
        uploadsDirectory = config().getString("uploads.directory")
        boolean jobProcessingEnabled = config().getBoolean("job_processing_enabled")
        def redis = new RedisDAO(RedisHelper.client(vertx, config()))

        vertx.executeBlocking({
            if (config().getBoolean("grakn.enabled")) {
                println "Ontology setup enabled"
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
                setupOntology(graknHost, graknPort, graknKeyspace)
                def grakn = new GraknDAO(vertx, redis, session)

                if (jobProcessingEnabled) {
                    println "Calculate job processing enabled"
                    def calculatorOptions = new DeploymentOptions().setConfig(config())
                    vertx.deployVerticle(new GraknCalculator(kue, redis, grakn), calculatorOptions)
                } else {
                    println "Calculate job processing disabled"
                }
            } else if (jobProcessingEnabled) {
                System.err.println("Job processing cannot be enabled with Grakn disabled")
                System.exit(-1)
            } else {
                println "Ontology setup disabled"
            }
            it.complete()

            if (jobProcessingEnabled) {
                println "Import job processing enabled"
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
                def grakn = new GraknDAO(vertx, redis, session)

                def importerOptions = new DeploymentOptions().setConfig(config())
                vertx.deployVerticle(new GraknImporter(kue, redis, grakn, uploadsDirectory), importerOptions)
            } else {
                println "Import job processing disabled"
            }

            if (config().getBoolean("launch_website")) {
                println "Launching GitDetective website"
                def options = new DeploymentOptions().setConfig(config())
                vertx.deployVerticle(new GitDetectiveWebsite(jobs, redis, router), options)
                vertx.deployVerticle(new GHArchiveSync(jobs, redis), options)
            }
        }, {
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
        vertx.eventBus().consumer("GetProjectReferenceLeaderboard", { request ->
            def timer = WebLauncher.metrics.timer("GetProjectReferenceLeaderboard")
            def context = timer.time()
            println "Getting project reference leaderboard"

            redis.getProjectReferenceLeaderboard(10, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                    context.stop()
                    return
                }

                println "Got project reference leaderboard - Size: " + it.result().size()
                request.reply(it.result())
                context.stop()
            })
        })
        vertx.eventBus().consumer("GetActiveJobs", { request ->
            def timer = WebLauncher.metrics.timer("GetActiveJobs")
            def context = timer.time()
            println "Getting active jobs"

            String order = "asc"
            Long from = 0
            Long to = 200
            String state = "active"
            //get 200 active jobs; return most recent 10
            //todo: smarter
            kue.jobRangeByState(state, from, to, order).setHandler({
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                    context.stop()
                    return
                }
                def allJobs = it.result()

                //filter out index imports (todo: use as status)
                allJobs.removeAll {
                    it.type == GraknImporter.GRAKN_INDEX_IMPORT_JOB_TYPE ||
                            it.type == GraknCalculator.GRAKN_CALCULATE_JOB_TYPE
                }
                //order by most recently updated
                allJobs = allJobs.sort({ it.updated_at }).reverse()
                JsonArray activeJobs = new JsonArray()
                //encode jobs
                allJobs.each { activeJobs.add(new JsonObject(Json.encode(it))) }
                //only most recent 10
                if (activeJobs.size() > 10) {
                    activeJobs = new JsonArray(activeJobs.take(10))
                }

                println "Got active jobs - Size: " + activeJobs.size()
                request.reply(activeJobs)
                context.stop()
            })
        })
        vertx.eventBus().consumer("GetLatestJobLog", { request ->
            def timer = WebLauncher.metrics.timer("GetLatestJobLog")
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()
            println "Getting job log: " + githubRepo

            jobs.getProjectLatestJob(githubRepo, {
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
                                println "Got job log. Size: " + it.result().size() + " - Repo: " + githubRepo
                                request.reply(new JsonObject().put("job_id", job.get().id).put("logs", it.result()))
                                context.stop()
                            }
                        })
                    } else {
                        println "Found no job logs"
                        request.reply(new JsonObject().put("job_id", -1).put("logs", new JsonArray()))
                        context.stop()
                    }
                }
            })
        })
        vertx.eventBus().consumer("CreateJob", { request ->
            def timer = WebLauncher.metrics.timer("CreateJob")
            def context = timer.time()
            println "Creating job"

            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()

            // user requested = highest priority
            jobs.createJob("IndexGithubProject", "User build job queued",
                    githubRepo, Priority.CRITICAL, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                    context.stop()
                } else {
                    def jobId = it.result().getId()
                    String result = new JsonObject()
                            .put("message", "User build job queued (id: $jobId)")
                            .put("id", jobId)
                    println "Created job: " + result

                    request.reply(result)
                    context.stop()
                }
            })
        })
        vertx.eventBus().consumer("TriggerRecalculation", { request ->
            def timer = WebLauncher.metrics.timer("TriggerRecalculation")
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()

            //check if can re-calculator
            vertx.eventBus().send("GetTriggerInformation", new JsonObject().put("github_repo", githubRepo), {
                def triggerInformation = it.result().body() as JsonObject
                if (triggerInformation.getBoolean("can_recalculate")) {
                    println "Triggering recalculation"

                    // user requested = highest priority
                    jobs.createJob(GraknCalculator.GRAKN_CALCULATE_JOB_TYPE,
                            "User reference recalculation queued",
                            new JsonObject().put("github_repository", githubRepo)
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
                            println "Created recalculation job: " + result
                            request.reply(result)
                            context.stop()
                        }
                    })
                }
            })
        })
        vertx.eventBus().consumer("GetProjectFileCount", { request ->
            def timer = WebLauncher.metrics.timer("GetProjectFileCount")
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()
            println "Getting project file count: " + githubRepo

            redis.getProjectFileCount(githubRepo, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    println "Got file count: " + it.result() + " - Repo: " + githubRepo
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer("GetProjectMethodVersionCount", { request ->
            def timer = WebLauncher.metrics.timer("GetProjectMethodVersionCount")
            def context = timer.time()
            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()
            println "Getting project method version count: " + githubRepo

            redis.getProjectMethodInstanceCount(githubRepo, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    println "Got method version count: " + it.result() + " - Repo: " + githubRepo
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer("GetProjectMostReferencedMethods", { request ->
            def timer = WebLauncher.metrics.timer("GetProjectMostReferencedMethods")
            def context = timer.time()
            println "Getting project most referenced methods"

            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()
            redis.getProjectMostExternalReferencedMethods(githubRepo, 10, {
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

                    println "Got project most referenced methods - Size: " + result.size()
                    request.reply(result)
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer("GetMethodMethodReferences", { request ->
            def timer = WebLauncher.metrics.timer("GetMethodMethodReferences")
            def context = timer.time()
            println "Getting method method references"

            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()
            def methodId = body.getString("method_id")
            def offset = body.getInteger("offset")

            redis.getMethodExternalMethodReferences(githubRepo, methodId, offset, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    println "Got method method references: " + it.result()
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer("GetProjectFirstIndexed", { request ->
            def timer = WebLauncher.metrics.timer("GetProjectFirstIndexed")
            def context = timer.time()
            println "Getting project first indexed"

            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()

            redis.getProjectFirstIndexed(githubRepo, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    println "Got project first indexed: " + it.result()
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer("GetProjectLastIndexed", { request ->
            def timer = WebLauncher.metrics.timer("GetProjectLastIndexed")
            def context = timer.time()
            println "Getting project last indexed"

            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()

            redis.getProjectLastIndexed(githubRepo, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    println "Got project last indexed: " + it.result()
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer("GetProjectLastIndexedCommitInformation", { request ->
            def timer = WebLauncher.metrics.timer("GetProjectLastIndexedCommitInformation")
            def context = timer.time()
            println "Getting project last indexed commit information"

            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()

            redis.getProjectLastIndexedCommitInformation(githubRepo, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    println "Got project last indexed commit information: " + it.result()
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer("GetProjectLastCalculated", { request ->
            def timer = WebLauncher.metrics.timer("GetProjectLastCalculated")
            def context = timer.time()
            println "Getting project last calculated"

            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()

            redis.getProjectLastCalculated(githubRepo, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    request.reply(it.cause())
                } else {
                    println "Got project last calculated: " + it.result()
                    request.reply(it.result())
                }
                context.stop()
            })
        })
        vertx.eventBus().consumer("GetTriggerInformation", { request ->
            def timer = WebLauncher.metrics.timer("GetTriggerInformation")
            def context = timer.time()
            println "Getting trigger information"

            def body = (JsonObject) request.body()
            def githubRepo = body.getString("github_repo").toLowerCase()

            def futures = new ArrayList<Future>()
            def canQueueFuture = Future.future()
            futures.add(canQueueFuture)
            jobs.getProjectLastQueued(githubRepo, canQueueFuture.completer())
            def canBuildFuture = Future.future()
            futures.add(canBuildFuture)
            redis.getProjectLastBuilt(githubRepo, canBuildFuture.completer())
            def canRecalculateFuture = Future.future()
            futures.add(canRecalculateFuture)
            redis.getProjectLastCalculated(githubRepo, canRecalculateFuture.completer())

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
        println "GitDetectiveService started"
    }

    static void setupOntology(String graknHost, int graknPort, String graknKeyspace) {
        def session = Grakn.session(graknHost + ":" + graknPort, graknKeyspace)
        def tx = session.open(GraknTxType.WRITE)
        def graql = tx.graql()
        def query = graql.parse(Resources.toString(Resources.getResource(
                "gitdetective-schema.gql"), Charsets.UTF_8))
        query.execute()
        tx.commit()
        session.close()
        println "Ontology setup"
    }

    private void downloadIndexFile(RoutingContext routingContext) {
        def uuid = randomUUID() as String
        println "Downloading index file. Index id: " + uuid

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