package io.gitdetective.web.dao

import io.gitdetective.web.work.importer.OpenSourceFunction
import io.vertx.core.AsyncResult
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.redis.RedisClient
import io.vertx.redis.op.RangeOptions

import java.time.Instant

import static io.gitdetective.web.WebServices.*

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class RedisDAO {

    public static final String NEW_PROJECT_FILE = "NewProjectFile"
    public static final String NEW_PROJECT_FUNCTION = "NewProjectFunction"
    public static final String NEW_REFERENCE = "NewReference"
    public static final String NEW_DEFINITION = "NewDefinition"
    private final static Logger log = LoggerFactory.getLogger(RedisDAO.class)
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

    void incrementCachedProjectFileCount(String githubRepository, long fileCount, Handler<AsyncResult> handler) {
        getProjectFileCount(githubRepository, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                redis.set("gitdetective:project:$githubRepository:project_file_count",
                        (it.result() + fileCount) as String, handler)
            }
        })
    }

    void getProjectMethodInstanceCount(String githubRepository, Handler<AsyncResult<Long>> handler) {
        redis.get("gitdetective:project:$githubRepository:project_method_instance_count", {
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

    void incrementCachedProjectMethodInstanceCount(String githubRepository, long methodInstanceCount,
                                                   Handler<AsyncResult> handler) {
        getProjectMethodInstanceCount(githubRepository, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                redis.set("gitdetective:project:$githubRepository:project_method_instance_count",
                        (it.result() + methodInstanceCount) as String, handler)
            }
        })
    }

    void appendJobToBuildHistory(String githubRepository, long jobId, Handler<AsyncResult<Void>> handler) {
        redis.lpush("gitdetective:project:$githubRepository:build_history", jobId as String, handler)
    }

    void getLatestJobId(String githubRepository, Handler<AsyncResult<Optional<Long>>> handler) {
        redis.lrange("gitdetective:project:$githubRepository:build_history", 0, 1, {
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

    void getProjectMostExternalReferencedMethods(String githubRepository, int topCount,
                                                 Handler<AsyncResult<JsonArray>> handler) {
        getOwnedFunctions(githubRepository, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def ownedFunctions = it.result()
                def rankedOwnedFunctions = new JsonArray()
                def futures = new ArrayList<Future>()
                for (int i = 0; i < ownedFunctions.size(); i++) {
                    def function = ownedFunctions.getJsonObject(i)
                    def fut = Future.future()
                    futures.add(fut)
                    getFunctionTotalExternalReferenceCount(function.getString("function_id"), {
                        if (it.failed()) {
                            fut.fail(it.cause())
                        } else {
                            rankedOwnedFunctions.add(function.put("external_reference_count", it.result()))
                            fut.complete()
                        }
                    })
                }

                CompositeFuture.all(futures).setHandler({
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        //sort and take top referenced functions
                        rankedOwnedFunctions = rankedOwnedFunctions.sort { a, b ->
                            return (a as JsonObject).getLong("external_reference_count") <=>
                                    (b as JsonObject).getLong("external_reference_count")
                        }.reverse() as JsonArray
                        rankedOwnedFunctions = rankedOwnedFunctions.take(topCount) as JsonArray
                        for (int i = 0; i < rankedOwnedFunctions.size(); i++) {
                            def functionId = rankedOwnedFunctions.getJsonObject(i).getString("function_id")
                            def qualifiedName = rankedOwnedFunctions.getJsonObject(i).getString("qualified_name")
                            rankedOwnedFunctions.getJsonObject(i)
                                    .put("id", functionId)
                                    .put("short_class_name", getShortQualifiedClassName(qualifiedName))
                                    .put("class_name", getQualifiedClassName(qualifiedName))
                                    .put("short_method_signature", getShortMethodSignature(qualifiedName))
                                    .put("method_signature", getMethodSignature(qualifiedName))
                                    .put("is_function", true)
                        }
                        handler.handle(Future.succeededFuture(rankedOwnedFunctions))
                        //todo: cache ranked owned functions
                    }
                })
            }
        })
    }

    void getMethodExternalReferences(String functionId, int offset, int limit,
                                     Handler<AsyncResult<JsonArray>> handler) {
        redis.lrange("gitdetective:osf:function_references:$functionId", offset, limit, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                if (it.result() == null) {
                    handler.handle(Future.succeededFuture(new JsonArray()))
                } else {
                    handler.handle(Future.succeededFuture(new JsonArray(it.result().toString())))
                }
            }
        })
    }

    void getFunctionTotalExternalReferenceCount(String functionId, Handler<AsyncResult<Long>> handler) {
        redis.llen("gitdetective:osf:function_references:$functionId", handler)
    }

    private void cacheFunctionReference(String functionId, JsonObject referenceFunction, Handler<AsyncResult> handler) {
        redis.lpush("gitdetective:osf:function_references:$functionId", referenceFunction.encode(), handler)
    }

    private void updateProjectReferenceLeaderboard(String githubRepository, long projectReferenceCount,
                                                   Handler<AsyncResult> handler) {
        redis.get("gitdetective:project:$githubRepository:project_external_method_reference_count", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def currentScore = it.result()
                if (currentScore == null) {
                    currentScore = 0
                } else {
                    currentScore = Long.parseLong(currentScore)
                }
                long newScore = (projectReferenceCount + currentScore)

                redis.set("gitdetective:project:$githubRepository:project_external_method_reference_count",
                        newScore as String, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        redis.zadd("gitdetective:project_reference_leaderboard", newScore, githubRepository, handler)
                    }
                })
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
                            .put("github_repository", list.getString(i).toLowerCase())
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
        redis.set("gitdetective:project:$githubRepository:project_first_indexed", now.toString(), handler)
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
        redis.set("gitdetective:project:$githubRepository:project_last_indexed", now.toString(), handler)
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
        redis.set("gitdetective:project:$githubRepository:project_last_indexed_commit_information", ob.encode(), handler)
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

    void setProjectLastBuilt(String githubRepository, Instant lastBuilt, Handler<AsyncResult> handler) {
        redis.set("gitdetective:project:$githubRepository:project_last_built", lastBuilt.toString(), handler)
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
        log.trace "Caching imported function: $filename"
        redis.publish(NEW_PROJECT_FILE, "$githubRepository|$filename|$fileId", handler)
    }

    void cacheProjectImportedFunction(String githubRepository, String functionName, String functionId, Handler<AsyncResult> handler) {
        log.trace "Caching imported function: $functionName"
        redis.publish(NEW_PROJECT_FUNCTION, "$githubRepository|$functionName|$functionId", handler)
    }

    void cacheProjectImportedDefinition(String fileId, String functionId, Handler<AsyncResult> handler) {
        log.trace "Caching imported definition: $fileId-$functionId"
        redis.publish(NEW_DEFINITION, "$fileId-$functionId", handler)
    }

    void cacheProjectImportedReference(String fileOrFunctionId, String functionId, Handler<AsyncResult> handler) {
        log.trace "Caching imported reference: $fileOrFunctionId-$functionId"
        redis.publish(NEW_REFERENCE, "$fileOrFunctionId-$functionId", handler)
    }

    private void addOwnedFunction(String githubRepository, String functionId, String qualifiedName,
                                  Handler<AsyncResult> handler) {
        log.trace "Adding owned function '$functionId' to owner: $githubRepository"
        redis.sadd("gitdetective:project:$githubRepository:ownedFunctions", new JsonObject()
                .put("function_id", functionId)
                .put("qualified_name", qualifiedName).encode(), handler)
    }

    void getOwnedFunctions(String githubRepository, Handler<AsyncResult<JsonArray>> handler) {
        redis.smembers("gitdetective:project:$githubRepository:ownedFunctions", {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def decodedArray = new JsonArray()
                def results = it.result() as JsonArray
                for (int i = 0; i < results.size(); i++) {
                    decodedArray.add(new JsonObject(results.getString(i)))
                }
                handler.handle(Future.succeededFuture(decodedArray))
            }
        })
    }

    void addFunctionOwner(String functionId, String qualifiedName, String githubRepository,
                          Handler<AsyncResult> handler) {
        log.trace "Adding owner '$githubRepository' to function: $functionId"
        redis.sadd("gitdetective:osf:owners:function:$functionId", githubRepository, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else if (it.result() == 1) {
                //make function owned by owner; check reference count; update project reference counts
                addOwnedFunction(githubRepository, functionId, qualifiedName, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        getFunctionTotalExternalReferenceCount(functionId, {
                            if (it.failed()) {
                                handler.handle(Future.failedFuture(it.cause()))
                            } else {
                                updateProjectReferenceLeaderboard(githubRepository, it.result(), handler)
                            }
                        })
                    }
                })
            } else {
                handler.handle(Future.succeededFuture())
            }
        })
    }

    private void getFunctionOwners(String functionId, Handler<AsyncResult<JsonArray>> handler) {
        log.trace "Getting owners of function: $functionId"
        redis.smembers("gitdetective:osf:owners:function:$functionId", handler)
    }

    void addFunctionReference(String functionId, JsonObject fileOrFunctionReference, Handler<AsyncResult> handler) {
        cacheFunctionReference(functionId, fileOrFunctionReference, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                getFunctionOwners(functionId, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        //update function owner in leaderboard
                        def owners = it.result() as JsonArray
                        def futures = new ArrayList<Future>()
                        for (int i = 0; i < owners.size(); i++) {
                            def owner = owners.getString(i)
                            def fut = Future.future()
                            futures.add(fut)
                            updateProjectReferenceLeaderboard(owner, 1, fut.completer())
                        }
                        CompositeFuture.all(futures).setHandler(handler)
                    }
                })
            }
        })
    }

    RedisClient getClient() {
        return redis
    }

}
