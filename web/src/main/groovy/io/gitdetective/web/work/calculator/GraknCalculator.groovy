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

import static io.gitdetective.web.WebServices.asPrettyTime
import static io.gitdetective.web.WebServices.logPrintln

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
            log.error "Calculate job error: " + it.body()
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
        def githubRepository = job.data.getString("github_repository").toLowerCase()
        def buildSkipped = job.data.getBoolean("build_skipped")
        if (buildSkipped == null) {
            buildSkipped = job.data.getBoolean("is_recalculation", false)
        }
        log.info "Calculating references for project: " + githubRepository + " - Build skipped: " + buildSkipped

        redis.getProjectLastCalculated(githubRepository, {
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
                    performCalculations(job, githubRepository, !buildSkipped, handler)
                }
            }
        })
    }

    private void performCalculations(Job job, String githubRepository, boolean indexed, Handler<AsyncResult> handler) {
        def timer = new Timer()
        def context = timer.time()

        logPrintln(job, "Calculating project most referenced methods")
        //get new externally references methods (save calc_import_round)
        grakn.getProjectNewExternalReferences(githubRepository, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                logPrintln(job, "Getting new external references took: " + asPrettyTime(context.stop()))

                //iterate methods and update calculated_import_round
                def myMethods = it.result()
                grakn.incrementFunctionComputedReferenceRounds(myMethods, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        logPrintln(job, "Incrementing computed reference rounds took: " + asPrettyTime(context.stop()))

                        //re-run each method with prev calc import round
                        grakn.getMethodNewExternalReferences(myMethods, {
                            if (it.failed()) {
                                handler.handle(Future.failedFuture(it.cause()))
                            } else {
                                //update cache
                                def cacheFutures = new ArrayList<Future>()
                                def refMethods = it.result()
                                def totalNewRefCount = 0
                                for (int i = 0; i < refMethods.size(); i++) {
                                    def method = myMethods.getJsonObject(i)
                                    def methodRefs = refMethods.getJsonArray(i)
                                    totalNewRefCount += methodRefs.size()

                                    def fut = Future.future()
                                    cacheFutures.add(fut)
                                    redis.cacheMethodReferences(githubRepository, method, methodRefs, fut.completer())
                                }

                                CompositeFuture.all(cacheFutures).setHandler({
                                    if (it.failed()) {
                                        handler.handle(Future.failedFuture(it.cause()))
                                    } else {
                                        //update project leaderboard
                                        redis.updateProjectReferenceLeaderboard(githubRepository, totalNewRefCount, {
                                            if (it.failed()) {
                                                handler.handle(Future.failedFuture(it.cause()))
                                            } else {
                                                logPrintln(job, "Most referenced methods took: " + asPrettyTime(context.stop()))
                                                WebLauncher.metrics.counter("GraknComputeTime").inc(context.stop())
                                                finalizeCalculations(job, indexed, githubRepository, handler)
                                            }
                                        })
                                    }
                                })
                            }
                        })
                    }
                })
            }
        })
    }

    private void finalizeCalculations(Job job, boolean indexed, String githubRepository, Handler<AsyncResult> handler) {
        logPrintln(job, "Successfully calculated references")
        if (indexed) {
            redis.getProjectFirstIndexed(githubRepository, {
                if (it.failed()) {
                    handler.handle(Future.failedFuture(it.cause()))
                    return
                }

                def futures = new ArrayList<Future>()
                if (it.result() == null) {
                    def fut = Future.future()
                    futures.add(fut)
                    redis.setProjectFirstIndexed(githubRepository, Instant.now(), fut.completer())
                }

                def fut2 = Future.future()
                futures.add(fut2)
                redis.setProjectLastIndexed(githubRepository, Instant.now(), fut2.completer())
                def fut3 = Future.future()
                futures.add(fut3)
                redis.setProjectLastIndexedCommitInformation(githubRepository,
                        job.data.getString("commit"), job.data.getInstant("commit_date"), fut3.completer())
                def fut4 = Future.future()
                futures.add(fut4)
                redis.setProjectLastCalculated(githubRepository, Instant.now(), fut4.completer())

                CompositeFuture.all(futures).setHandler({
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        logPrintln(job, "Cached calculated references")
                        handler.handle(Future.succeededFuture())
                    }
                })
            })
        } else {
            redis.setProjectLastCalculated(githubRepository, Instant.now(), {
                if (it.failed()) {
                    handler.handle(Future.failedFuture(it.cause()))
                } else {
                    logPrintln(job, "Cached calculated references")
                    handler.handle(Future.succeededFuture())
                }
            })
        }
    }

}
