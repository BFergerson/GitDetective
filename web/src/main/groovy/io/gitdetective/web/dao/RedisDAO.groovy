package io.gitdetective.web.dao

import io.gitdetective.web.work.importer.OpenSourceFunction
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.redis.RedisClient
import io.vertx.redis.op.RangeOptions

import java.time.Instant

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class RedisDAO {

    public static final String NEW_PROJECT_FILE = "NewProjectFile"
    public static final String NEW_PROJECT_FUNCTION = "NewProjectFunction"
    public static final String NEW_REFERENCE = "NewReference"
    public static final String NEW_DEFINITION = "NewDefinition"
    private final RedisClient redis

    RedisDAO(RedisClient redis) {
        this.redis = redis

        //verify redis connection
        redis.ping({
            if (it.failed()) {
                throw new RuntimeException(it.cause())
            }
        })
    }

    void getProjectFileCount(String githubRepo, Handler<AsyncResult<Long>> handler) {
        redis.get("gitdetective:project:$githubRepo:project_file_count", {
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

    void incrementCachedProjectFileCount(String githubRepo, long fileCount, Handler<AsyncResult> handler) {
        getProjectFileCount(githubRepo, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                redis.set("gitdetective:project:$githubRepo:project_file_count",
                        (it.result() + fileCount) as String, handler)
            }
        })
    }

    void getProjectMethodInstanceCount(String githubRepo, Handler<AsyncResult<Long>> handler) {
        redis.get("gitdetective:project:$githubRepo:project_method_instance_count", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result()
                if (result == null) {
                    result = 0
                }
                handler.handle(Future.succeededFuture(result as int))
            }
        })
    }

    void incrementCachedProjectMethodInstanceCount(String githubRepo, long methodInstanceCount,
                                                   Handler<AsyncResult> handler) {
        getProjectMethodInstanceCount(githubRepo, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                redis.set("gitdetective:project:$githubRepo:project_method_instance_count",
                        (it.result() + methodInstanceCount) as String, handler)
            }
        })
    }

    void appendJobToBuildHistory(String githubRepo, long jobId, Handler<AsyncResult<Void>> handler) {
        redis.lpush("gitdetective:project:$githubRepo:build_history", jobId as String, handler)
    }

    void getLatestJobId(String githubRepo, Handler<AsyncResult<Optional<Long>>> handler) {
        redis.lrange("gitdetective:project:$githubRepo:build_history", 0, 1, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def jsonArray = it.result() as JsonArray
                if (jsonArray.isEmpty()) {
                    handler.handle(Future.succeededFuture(Optional.empty()))
                } else {
                    def jobLogId = jsonArray.getString(0) as long
                    handler.handle(Future.succeededFuture(Optional.of(jobLogId)))
                }
            }
        })
    }

    void getProjectMostExternalReferencedMethods(String githubRepo, int topCount, Handler<AsyncResult<JsonArray>> handler) {
        redis.zrevrange("gitdetective:project:$githubRepo:method_method_reference_leaderboard", 0, topCount - 1, RangeOptions.WITHSCORES, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def list = it.result() as JsonArray
                def rtnArray = new JsonArray()
                for (int i = 0; i < list.size(); i += 2) {
                    rtnArray.add(new JsonObject(list.getString(i))
                            .put("external_reference_count", list.getString(i + 1) as int))
                }
                handler.handle(Future.succeededFuture(rtnArray))
            }
        })
    }

    void getMethodExternalMethodReferences(String githubRepo, String methodId, int offset,
                                           Handler<AsyncResult<JsonArray>> handler) {
        redis.get("gitdetective:project:$githubRepo:method_method_references:" + methodId + ":" + offset, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                if (it.result() == null) {
                    handler.handle(Future.succeededFuture(new JsonArray()))
                } else {
                    handler.handle(Future.succeededFuture(new JsonArray(it.result())))
                }
            }
        })
    }

    void cacheMethodMethodReferences(String githubRepo, String methodId, int offset,
                                     JsonArray methodReferences, Handler<AsyncResult> handler) {
        redis.set("gitdetective:project:$githubRepo:method_method_references:" + methodId + ":" + offset,
                methodReferences.encode(), handler)
    }

    void cacheMethodMethodReferenceCount(String githubRepo, JsonObject method, long referenceCount,
                                         Handler<AsyncResult> handler) {
        redis.set("gitdetective:project:$githubRepo:method_method_reference_count:" + method.getString("id"),
                referenceCount as String, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def methodDupe = method.copy()
                methodDupe.remove("commit_sha1") //don't care about which commit method came from
                redis.zadd("gitdetective:project:$githubRepo:method_method_reference_leaderboard", referenceCount,
                        methodDupe.encode(), handler)
            }
        })
    }

    void updateProjectReferenceLeaderboard(String githubRepo, long projectMethodReferenceCount,
                                           Handler<AsyncResult> handler) {
        redis.set("gitdetective:project:$githubRepo:project_external_method_reference_count",
                projectMethodReferenceCount as String, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                redis.zadd("gitdetective:project_reference_leaderboard", projectMethodReferenceCount, githubRepo, handler)
            }
        })
    }

    void getProjectReferenceLeaderboard(int topCount, Handler<AsyncResult<JsonArray>> handler) {
        redis.zrevrange("gitdetective:project_reference_leaderboard", 0, topCount - 1, RangeOptions.WITHSCORES, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def list = it.result() as JsonArray
                def rtnArray = new JsonArray()
                for (int i = 0; i < list.size(); i += 2) {
                    rtnArray.add(new JsonObject()
                            .put("github_repo", list.getString(i).toLowerCase())
                            .put("value", list.getString(i + 1)))
                }

                handler.handle(Future.succeededFuture(rtnArray))
            }
        })
    }

    void getLastArchiveSync(Handler<AsyncResult<String>> handler) {
        redis.get("gitdetective:last_archive_sync", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result() as String
                handler.handle(Future.succeededFuture(result))
            }
        })
    }

    void setLastArchiveSync(String now, Handler<AsyncResult> handler) {
        redis.set("gitdetective:last_archive_sync", now, handler)
    }

    void getProjectFirstIndexed(String githubRepo, Handler<AsyncResult<String>> handler) {
        redis.get("gitdetective:project:$githubRepo:project_first_indexed", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result() as String
                handler.handle(Future.succeededFuture(result))
            }
        })
    }

    void setProjectFirstIndexed(String githubRepo, Instant now, Handler<AsyncResult> handler) {
        redis.set("gitdetective:project:$githubRepo:project_first_indexed", now.toString(), handler)
    }

    void getProjectLastIndexed(String githubRepo, Handler<AsyncResult<String>> handler) {
        redis.get("gitdetective:project:$githubRepo:project_last_indexed", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result() as String
                handler.handle(Future.succeededFuture(result))
            }
        })
    }

    void setProjectLastIndexed(String githubRepo, Instant now, Handler<AsyncResult> handler) {
        redis.set("gitdetective:project:$githubRepo:project_last_indexed", now.toString(), handler)
    }

    void getProjectLastIndexedCommitInformation(String githubRepo, Handler<AsyncResult<JsonObject>> handler) {
        redis.get("gitdetective:project:$githubRepo:project_last_indexed_commit_information", {
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

    void setProjectLastIndexedCommitInformation(String githubRepo, String commitSha1, Instant commitDate,
                                                Handler<AsyncResult> handler) {
        def ob = new JsonObject()
                .put("commit", commitSha1)
                .put("commit_date", commitDate)
        redis.set("gitdetective:project:$githubRepo:project_last_indexed_commit_information", ob.encode(), handler)
    }

    void getProjectLastBuilt(String githubRepo, Handler<AsyncResult<String>> handler) {
        redis.get("gitdetective:project:$githubRepo:project_last_built", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result() as String
                handler.handle(Future.succeededFuture(result))
            }
        })
    }

    void setProjectLastBuilt(String githubRepo, Instant lastBuilt, Handler<AsyncResult> handler) {
        redis.set("gitdetective:project:$githubRepo:project_last_built", lastBuilt.toString(), handler)
    }

    void getProjectLastCalculated(String githubRepo, Handler<AsyncResult<String>> handler) {
        redis.get("gitdetective:project:$githubRepo:project_last_calculated", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result() as String
                handler.handle(Future.succeededFuture(result))
            }
        })
    }

    void setProjectLastCalculated(String githubRepo, Instant now, Handler<AsyncResult> handler) {
        redis.set("gitdetective:project:$githubRepo:project_last_calculated", now.toString(), handler)
    }

    void getComputeTime(Handler<AsyncResult<Long>> handler) {
        redis.get("gitdetective:compute_time", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                if (it.result() == null) {
                    handler.handle(Future.succeededFuture(0))
                } else {
                    handler.handle(Future.succeededFuture(Long.valueOf(it.result())))
                }
            }
        })
    }

    long cacheComputeTime(long uptime) {
        redis.set("gitdetective:compute_time", uptime + "", {
            if (it.failed()) {
                it.cause().printStackTrace()
            }
        })
        return uptime
    }

    void getDefinitionCount(Handler<AsyncResult<Long>> handler) {
        redis.get("gitdetective:definition_count", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                if (it.result() == null) {
                    handler.handle(Future.succeededFuture(0))
                } else {
                    handler.handle(Future.succeededFuture(Long.valueOf(it.result())))
                }
            }
        })
    }

    long cacheDefinitionCount(long definitionCount) {
        redis.set("gitdetective:definition_count", Long.toString(definitionCount), {
            if (it.failed()) {
                it.cause().printStackTrace()
            }
        })
        return definitionCount
    }

    void getReferenceCount(Handler<AsyncResult<Long>> handler) {
        redis.get("gitdetective:reference_count", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                if (it.result() == null) {
                    handler.handle(Future.succeededFuture(0))
                } else {
                    handler.handle(Future.succeededFuture(Long.valueOf(it.result())))
                }
            }
        })
    }

    long cacheReferenceCount(long referenceCount) {
        redis.set("gitdetective:reference_count", Long.toString(referenceCount), {
            if (it.failed()) {
                it.cause().printStackTrace()
            }
        })
        return referenceCount
    }

    void getProjectCount(Handler<AsyncResult<Long>> handler) {
        redis.get("gitdetective:project_count", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                if (it.result() == null) {
                    handler.handle(Future.succeededFuture(0))
                } else {
                    handler.handle(Future.succeededFuture(Long.valueOf(it.result())))
                }
            }
        })
    }

    long cacheProjectCount(long projectCount) {
        redis.set("gitdetective:project_count", Long.toString(projectCount), {
            if (it.failed()) {
                it.cause().printStackTrace()
            }
        })
        return projectCount
    }

    void getFileCount(Handler<AsyncResult<Long>> handler) {
        redis.get("gitdetective:file_count", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                if (it.result() == null) {
                    handler.handle(Future.succeededFuture(0))
                } else {
                    handler.handle(Future.succeededFuture(Long.valueOf(it.result())))
                }
            }
        })
    }

    long cacheFileCount(long fileCount) {
        redis.set("gitdetective:file_count", Long.toString(fileCount), {
            if (it.failed()) {
                it.cause().printStackTrace()
            }
        })
        return fileCount
    }

    void getMethodCount(Handler<AsyncResult<Long>> handler) {
        redis.get("gitdetective:method_count", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                if (it.result() == null) {
                    handler.handle(Future.succeededFuture(0))
                } else {
                    handler.handle(Future.succeededFuture(Long.valueOf(it.result())))
                }
            }
        })
    }

    long cacheMethodCount(long methodCount) {
        redis.set("gitdetective:method_count", Long.toString(methodCount), {
            if (it.failed()) {
                it.cause().printStackTrace()
            }
        })
        return methodCount
    }

    void cacheOpenSourceFunction(String functionName, OpenSourceFunction osFunc, Handler<AsyncResult> handler) {
        redis.set("gitdetective:osf:" + functionName, Json.encode(osFunc), handler)
    }

    void getOpenSourceFunction(String functionName, Handler<AsyncResult<Optional<OpenSourceFunction>>> handler) {
        redis.get("gitdetective:osf:" + functionName, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def result = it.result()
                if (result == null) {
                    handler.handle(Future.succeededFuture(Optional.empty()))
                } else {
                    def ob = new JsonObject(it.result())
                    def osFunc = new OpenSourceFunction(ob.getString("functionId"),
                            ob.getString("functionDefinitionsId"), ob.getString("functionReferencesId"))
                    handler.handle(Future.succeededFuture(Optional.of(osFunc)))
                }
            }
        })
    }

    void cacheProjectImportedFile(String githubRepository, String filename, String fileId, Handler<AsyncResult> handler) {
        redis.publish(NEW_PROJECT_FILE, "$githubRepository|$filename|$fileId", handler)
    }

    void cacheProjectImportedFunction(String githubRepository, String functionName, String functionId,
                                      Handler<AsyncResult> handler) {
        redis.publish(NEW_PROJECT_FUNCTION, "$githubRepository|$functionName|$functionId", handler)
    }

    void cacheProjectImportedDefinition(String fileId, String functionId, Handler<AsyncResult> handler) {
        redis.publish(NEW_DEFINITION, "$fileId-$functionId", handler)
    }

    void cacheProjectImportedReference(String fileOrFunctionId, String functionId, Handler<AsyncResult> handler) {
        redis.publish(NEW_REFERENCE, "$fileOrFunctionId-$functionId", handler)
    }

    RedisClient getClient() {
        return redis
    }

}
