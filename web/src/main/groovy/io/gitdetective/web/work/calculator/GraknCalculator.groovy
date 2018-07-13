package io.gitdetective.web.work.calculator

import com.codahale.metrics.Timer
import io.gitdetective.web.WebLauncher
import io.gitdetective.web.dao.GraknDAO
import io.gitdetective.web.dao.RedisDAO
import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.*
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory

import java.time.Instant
import java.time.temporal.ChronoUnit

import static io.gitdetective.web.Utils.asPrettyTime
import static io.gitdetective.web.Utils.logPrintln

/**
 * Runs queries on Grakn to calculate project reference data which
 * is immediately cached for displaying on website.
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GraknCalculator extends AbstractVerticle {

    public static final String GRAKN_CALCULATE_JOB_TYPE = "CalculateGithubProject"
    private final static Logger log = LoggerFactory.getLogger(GraknCalculator.class)
    private final Kue kue
    private final RedisDAO redis
    private final GraknDAO grakn
    public int PROJECT_RECALCULATE_WAIT_TIME //hours

    GraknCalculator(Kue kue, RedisDAO redis, GraknDAO grakn) {
        this.kue = kue
        this.redis = redis
        this.grakn = grakn
    }

    @Override
    void start() throws Exception {
        def calculatorConfig = config().getJsonObject("calculator")
        PROJECT_RECALCULATE_WAIT_TIME = calculatorConfig.getInteger("project_recalculate_wait_time")

        def graknCalculateMeter = WebLauncher.metrics.meter("GraknCalculateJobProcessSpeed")
        kue.on("error", {
            System.err.println("Calculate job error: " + it.body())
        })
        kue.process(GRAKN_CALCULATE_JOB_TYPE, calculatorConfig.getInteger("thread_count"), { calculateJob ->
            graknCalculateMeter.mark()
            log.info "Calculate job rate: " + (graknCalculateMeter.oneMinuteRate * 60) +
                    " per/min - Thread: " + Thread.currentThread().name

            vertx.executeBlocking({ blocking ->
                if (calculateJob.data.getBoolean("is_recalculation")) {
                    processCalculateJob(calculateJob, {
                        if (it.failed()) {
                            calculateJob.done(it.cause())
                        } else {
                            calculateJob.done()
                        }
                        blocking.complete()
                    })
                } else {
                    calculateJob.removeOnComplete = true
                    def job = new Job(calculateJob.data)
                    processCalculateJob(job, { result ->
                        if (result.failed()) {
                            job.done(result.cause())
                            job.failed().setHandler({
                                calculateJob.done(result.cause())
                                blocking.complete()
                            })
                        } else {
                            job.done()
                            job.complete().setHandler({
                                calculateJob.done()
                                blocking.complete()
                            })
                        }
                    })
                }
            }, false, {
                //do nothing
            })
        })
        log.info "GraknCalculator started"
    }

    private void processCalculateJob(Job job, Handler<AsyncResult> handler) {
        def githubRepo = job.data.getString("github_repository").toLowerCase()
        def buildSkipped = job.data.getBoolean("build_skipped")
        if (buildSkipped == null) {
            buildSkipped = false
        }
        log.info "Calculating references for project: " + githubRepo + " - Build skipped: " + buildSkipped

        redis.getProjectLastCalculated(githubRepo, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                boolean skipCalculation = false
                if (it.result() != null) {
                    def lastCalculated = Instant.parse(it.result())
                    if (lastCalculated.plus(PROJECT_RECALCULATE_WAIT_TIME, ChronoUnit.HOURS).isAfter(Instant.now())) {
                        if (!job.data.getBoolean("admin_triggered")) {
                            skipCalculation = true
                            logPrintln(job, "Skipping project reference calculation. Limited to once a day")
                        }
                    }
                }

                if (skipCalculation) {
                    logPrintln(job, "Skipped calculating references")
                    handler.handle(Future.succeededFuture())
                } else {
                    performCalculations(job, githubRepo, !buildSkipped, handler)
                }
            }
        })
    }

    private void performCalculations(Job job, String githubRepo, boolean indexed, Handler<AsyncResult> handler) {
        def calcConfig = config().getJsonObject("calculator")
        def futures = new ArrayList<Future>()

        //method references
        if (calcConfig.getBoolean("project_external_method_reference_count"))
            futures.add(calculateProjectExternalMethodReferenceCount(job))
        if (calcConfig.getBoolean("project_most_referenced_methods"))
            futures.add(getProjectMostExternalReferencedMethods(job))

        def timer = new Timer()
        def context = timer.time()
        CompositeFuture.all(futures).setHandler({
            WebLauncher.metrics.counter("GraknComputeTime").inc(context.stop())
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
                return
            }

            logPrintln(job, "Successfully calculated references")
            if (indexed) {
                redis.getProjectFirstIndexed(githubRepo, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                        return
                    }

                    def moreFutures = new ArrayList<Future>()
                    if (it.result() == null) {
                        def fut = Future.future()
                        moreFutures.add(fut)
                        redis.setProjectFirstIndexed(githubRepo, Instant.now(), fut.completer())
                    }

                    def fut2 = Future.future()
                    moreFutures.add(fut2)
                    redis.setProjectLastIndexed(githubRepo, Instant.now(), fut2.completer())
                    def fut3 = Future.future()
                    moreFutures.add(fut3)
                    redis.setProjectLastIndexedCommitInformation(githubRepo,
                            job.data.getString("commit"), job.data.getInstant("commit_date"), fut3.completer())
                    def fut4 = Future.future()
                    moreFutures.add(fut4)
                    redis.setProjectLastCalculated(githubRepo, Instant.now(), fut4.completer())

                    CompositeFuture.all(moreFutures).setHandler({
                        if (it.failed()) {
                            handler.handle(Future.failedFuture(it.cause()))
                        } else {
                            logPrintln(job, "Cached calculated references")
                            handler.handle(Future.succeededFuture())
                        }
                    })
                })
            } else {
                redis.setProjectLastCalculated(githubRepo, Instant.now(), {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        logPrintln(job, "Cached calculated references")
                        handler.handle(Future.succeededFuture())
                    }
                })
            }
        })
    }

    private Future getProjectMostExternalReferencedMethods(Job job) {
        def timer = WebLauncher.metrics.timer("CalculateProjectMostExternalReferencedMethods")
        def context = timer.time()
        logPrintln(job, "Calculating project most referenced methods")
        def githubRepo = job.data.getString("github_repository").toLowerCase()

        def future = Future.future()
        grakn.getProjectMostExternalReferencedMethods(githubRepo, {
            if (it.failed()) {
                it.cause().printStackTrace()
                future.complete(it.cause())
            } else {
                logPrintln(job, "Most referenced methods took: " + asPrettyTime(context.stop()))
                future.complete()
            }
        })
        return future
    }

    private Future calculateProjectExternalMethodReferenceCount(Job job) {
        def timer = WebLauncher.metrics.timer("CalculateProjectExternalMethodReferenceCount")
        def context = timer.time()
        logPrintln(job, "Calculating external method reference count")
        def githubRepo = job.data.getString("github_repository").toLowerCase()

        def future = Future.future()
        grakn.getProjectExternalMethodReferenceCount(githubRepo, {
            if (it.failed()) {
                it.cause().printStackTrace()
                future.complete(it.cause())
            } else {
                logPrintln(job, "External method reference count took: " + asPrettyTime(context.stop()))
                future.complete()
            }
        })
        return future
    }

}
