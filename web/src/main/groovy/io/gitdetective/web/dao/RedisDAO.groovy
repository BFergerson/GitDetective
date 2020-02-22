package io.gitdetective.web.dao

import io.vertx.blueprint.kue.util.RedisHelper
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.redis.RedisClient
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.op.RangeOptions

import java.time.Instant

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class RedisDAO {

    private final static Logger log = LoggerFactory.getLogger(RedisDAO.class)
    private final RedisAPI redis

    RedisDAO(Redis redis) {
        this.redis = RedisAPI.api(redis)

        //verify redis connection
        redis.connect({
            this.redis.ping(Collections.emptyList(), {
                if (it.failed()) {
                    throw new RuntimeException(it.cause())
                }
            })
        })
    }

    void getProjectFileCount(String githubRepository, Handler<AsyncResult<Long>> handler) {
        log.trace "Getting project file count: $githubRepository"
        redis.get("gitdetective:project:$githubRepository:project_file_count", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result()
                if (result == null) {
                    result = 0
                }
                log.trace "Getting project file count: $result"
                handler.handle(Future.succeededFuture(result as long))
            }
        })
    }

    void getProjectMethodInstanceCount(String githubRepository, Handler<AsyncResult<Long>> handler) {
        redis.get("gitdetective:project:$githubRepository:project_function_instance_count", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result()
                if (result == null) {
                    result = 0
                }
                handler.handle(Future.succeededFuture(result as long))
            }
        })
    }

    void appendJobToBuildHistory(String githubRepository, long jobId, Handler<AsyncResult<Void>> handler) {
        log.trace "Adding job $jobId from '$githubRepository' build history"
        redis.lpush(Arrays.asList("gitdetective:project:$githubRepository:build_history".toString(), Long.toString(jobId)), handler)
    }

    void removeLatestJobFromBuildHistory(String githubRepository, long jobId, Handler<AsyncResult<Void>> handler) {
        log.info "Removing job $jobId from '$githubRepository' build history"
        redis.lrem("gitdetective:project:$githubRepository:build_history", "1", jobId as String, handler)
    }

    void getLatestJobId(String githubRepository, Handler<AsyncResult<Optional<Long>>> handler) {
        log.trace "Getting latest job id for project $githubRepository"
        redis.lrange("gitdetective:project:$githubRepository:build_history", "0", "1", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def jsonArray = RedisHelper.toJsonArray(it.result())
                if (jsonArray.isEmpty()) {
                    handler.handle(Future.succeededFuture(Optional.empty()))
                } else {
                    def jobLogId = jsonArray.getString(0) as long
                    handler.handle(Future.succeededFuture(Optional.of(jobLogId)))
                }
            }
        })
    }

    void getProjectReferenceLeaderboard(int topCount, Handler<AsyncResult<JsonArray>> handler) {
        redis.zrevrange(Arrays.asList("gitdetective:project_reference_leaderboard", "0", Long.toString(topCount - 1),
                RangeOptions.WITHSCORES.toString()), {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def list = RedisHelper.toJsonArray(it.result())
                def rtnArray = new JsonArray()
                for (int i = 0; i < list.size(); i += 2) {
                    rtnArray.add(new JsonObject()
                            .put("github_repository", list.getString(i).toLowerCase())
                            .put("value", list.getString(i + 1)))
                }

                handler.handle(Future.succeededFuture(rtnArray))
            }
        })
    }

    protected void getLastArchiveSync(Handler<AsyncResult<String>> handler) {
        redis.get("gitdetective:last_archive_sync", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result() as String
                handler.handle(Future.succeededFuture(result))
            }
        })
    }

    protected void setLastArchiveSync(String now, Handler<AsyncResult> handler) {
        redis.set(["gitdetective:last_archive_sync", now], handler)
    }

    void getProjectFirstIndexed(String githubRepository, Handler<AsyncResult<String>> handler) {
        redis.get("gitdetective:project:$githubRepository:project_first_indexed", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result() as String
                handler.handle(Future.succeededFuture(result))
            }
        })
    }

    void setProjectFirstIndexed(String githubRepository, Instant now, Handler<AsyncResult> handler) {
        redis.set(["gitdetective:project:$githubRepository:project_first_indexed", now.toString()], handler)
    }

    void getProjectLastIndexed(String githubRepository, Handler<AsyncResult<String>> handler) {
        redis.get("gitdetective:project:$githubRepository:project_last_indexed", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result() as String
                handler.handle(Future.succeededFuture(result))
            }
        })
    }

    void setProjectLastIndexed(String githubRepository, Instant now, Handler<AsyncResult> handler) {
        redis.set(["gitdetective:project:$githubRepository:project_last_indexed", now.toString()], handler)
    }

    void getProjectLastIndexedCommitInformation(String githubRepository, Handler<AsyncResult<JsonObject>> handler) {
        redis.get("gitdetective:project:$githubRepository:project_last_indexed_commit_information", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result() as String
                if (result == null) {
                    handler.handle(Future.succeededFuture(null) as AsyncResult<JsonObject>)
                } else {
                    handler.handle(Future.succeededFuture(new JsonObject(result)))
                }
            }
        })
    }

    void setProjectLastIndexedCommitInformation(String githubRepository, String commitSha1, Instant commitDate,
                                                Handler<AsyncResult> handler) {
        def ob = new JsonObject()
                .put("commit", Objects.requireNonNull(commitSha1))
                .put("commit_date", Objects.requireNonNull(commitDate))
        redis.set(["gitdetective:project:$githubRepository:project_last_indexed_commit_information", ob.encode()], handler)
    }

    void getProjectLastBuilt(String githubRepository, Handler<AsyncResult<String>> handler) {
        redis.get("gitdetective:project:$githubRepository:project_last_built", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result() as String
                handler.handle(Future.succeededFuture(result))
            }
        })
    }

    protected void setProjectLastBuilt(String githubRepository, Instant lastBuilt, Handler<AsyncResult> handler) {
        redis.set(Arrays.asList("gitdetective:project:$githubRepository:project_last_built".toString(), lastBuilt.toString()), handler)
    }
}
