package io.gitdetective.web.dao

import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.Job
import io.vertx.blueprint.kue.queue.KueVerticle
import io.vertx.blueprint.kue.queue.Priority
import io.vertx.blueprint.kue.util.RedisHelper
import io.vertx.core.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

import java.time.Instant

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class JobsDAO {

    private final RedisDAO redis
    private Kue kue

    JobsDAO(Vertx vertx, JsonObject config) {
        this(vertx, config, {})
    }

    JobsDAO(Vertx vertx, JsonObject config, Handler<AsyncResult> handler) {
        if (config.getJsonObject("jobs_server") != null) {
            def redisClient = RedisHelper.client(vertx, config.getJsonObject("jobs_server"))
            redis = new RedisDAO(redisClient)
        } else {
            def redisClient = RedisHelper.client(vertx, config)
            redis = new RedisDAO(redisClient)
        }

        def kueOptions = new DeploymentOptions().setConfig(config)
        if (config.getJsonObject("jobs_server") != null) {
            kueOptions.config = config.getJsonObject("jobs_server")
        }

        kue = Kue.createQueue(vertx, kueOptions.config)
        vertx.deployVerticle(new KueVerticle(kue), kueOptions, {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }
            handler.handle(Future.succeededFuture())
        })
    }

    void createJob(String jobType, String initialMessage, String githubRepository, Handler<AsyncResult<Job>> handler) {
        createJob(jobType, initialMessage, githubRepository, Priority.NORMAL, handler)
    }

    void createJob(String jobType, String initialMessage, String githubRepository, Priority priority,
                   Handler<AsyncResult<Job>> handler) {
        createJob(jobType, initialMessage, new JsonObject().put("github_repository", githubRepository), priority, handler)
    }

    void createJob(String jobType, String initialMessage, JsonObject data, Priority priority,
                   Handler<AsyncResult<Job>> handler) {
        kue.createJob(jobType, data)
                .setMax_attempts(0)
                .setPriority(priority)
                .save().setHandler({ job ->
            if (job.failed()) {
                handler.handle(Future.failedFuture(job.cause()))
            } else {
                job.result().log(initialMessage + " (id: " + job.result().id + ")").setHandler({
                    if (jobType == "ImportGithubProject") {
                        //shouldn't ever start at import... either way import stage isn't associated to build history
                        //comes from either index or calculate
                        handler.handle(Future.succeededFuture(job.result()))
                    } else {
                        redis.appendJobToBuildHistory(data.getString("github_repository"), job.result().id, {
                            if (it.failed()) {
                                handler.handle(Future.failedFuture(it.cause()))
                            } else {
                                handler.handle(Future.succeededFuture(job.result()))
                            }
                        })
                    }
                })
            }
        })
    }

    void getProjectLatestJob(String githubRepository, Handler<AsyncResult<Optional<Job>>> handler) {
        redis.getLatestJobId(githubRepository, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def latestJobId = it.result() as Optional<Long>
                if (latestJobId.isPresent()) {
                    kue.getJob(latestJobId.get()).setHandler({
                        if (it.failed()) {
                            handler.handle(Future.failedFuture(it.cause()))
                        } else if (it.result().isPresent()) {
                            handler.handle(Future.succeededFuture(Optional.of(it.result().get())))
                        } else {
                            //remove from build history
                            redis.removeLatestJobFromBuildHistory(githubRepository, latestJobId.get(), {
                                //return new latest
                                getProjectLatestJob(githubRepository, handler)
                            })
                        }
                    })
                } else {
                    handler.handle(Future.succeededFuture(Optional.empty()))
                }
            }
        })
    }

    void getJobLogs(long jobId, Handler<AsyncResult<JsonArray>> handler) {
        kue.getJobLog(jobId).setHandler({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                handler.handle(Future.succeededFuture(it.result()))
            }
        })
    }

    void getProjectLastQueued(String githubRepository, Handler<AsyncResult<Optional<Instant>>> handler) {
        getProjectLatestJob(githubRepository, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else if (it.result().isPresent()) {
                handler.handle(Future.succeededFuture(Optional.of(Instant.ofEpochMilli(it.result().get().created_at))))
            } else {
                handler.handle(Future.succeededFuture(Optional.empty()))
            }
        })
    }

    void getActiveCount(String jobType, Handler<AsyncResult<Long>> handler) {
        kue.activeCount(jobType).setHandler(handler)
    }

    void getProjectLastIndexedCommitInformation(String githubRepository, Handler<AsyncResult<JsonObject>> handler) {
        redis.getProjectLastIndexedCommitInformation(githubRepository, handler)
    }

    void getProjectLastBuilt(String githubRepository, Handler<AsyncResult<String>> handler) {
        redis.getProjectLastBuilt(githubRepository, handler)
    }

    void setProjectLastBuilt(String githubRepository, Instant lastBuilt, Handler<AsyncResult> handler) {
        redis.setProjectLastBuilt(githubRepository, lastBuilt, handler)
    }

    void getLastArchiveSync(Handler<AsyncResult<String>> handler) {
        redis.getLastArchiveSync(handler)
    }

    void setLastArchiveSync(String now, Handler<AsyncResult> handler) {
        redis.setLastArchiveSync(now, handler)
    }

    Kue getKue() {
        return kue
    }

}
