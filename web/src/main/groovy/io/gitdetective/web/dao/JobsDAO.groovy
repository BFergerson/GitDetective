package io.gitdetective.web.dao

import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.Job
import io.vertx.blueprint.kue.queue.Priority
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

import java.time.Instant

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class JobsDAO {

    private final Kue kue
    private final RedisDAO redis

    JobsDAO(Kue kue, RedisDAO redis) {
        this.kue = kue
        this.redis = redis
    }

    void createJob(String jobType, String initialMessage, String githubRepo, Handler<AsyncResult<Job>> handler) {
        createJob(jobType, initialMessage, githubRepo, Priority.NORMAL, handler)
    }

    void createJob(String jobType, String initialMessage,
                   String githubRepo, Priority priority, Handler<AsyncResult<Job>> handler) {
        createJob(jobType, initialMessage, new JsonObject().put("github_repository", githubRepo), priority, handler)
    }

    void createJob(String jobType, String initialMessage, JsonObject data,
                   Priority priority, Handler<AsyncResult<Job>> handler) {
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

    void getProjectLatestJob(String githubRepo, Handler<AsyncResult<Optional<Job>>> handler) {
        redis.getLatestJobId(githubRepo, {
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
                            handler.handle(Future.succeededFuture(Optional.empty()))
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

    void getProjectLastQueued(String githubRepo, Handler<AsyncResult<Optional<Instant>>> handler) {
        getProjectLatestJob(githubRepo, {
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

}
