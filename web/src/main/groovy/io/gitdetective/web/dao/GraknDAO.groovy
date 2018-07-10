package io.gitdetective.web.dao

import ai.grakn.GraknSession
import ai.grakn.GraknTxType
import ai.grakn.graql.QueryBuilder
import ai.grakn.graql.internal.query.QueryAnswer
import com.google.common.base.Charsets
import com.google.common.io.Resources
import io.gitdetective.web.WebLauncher
import io.gitdetective.web.work.importer.OpenSourceFunction
import io.vertx.core.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

import static io.gitdetective.web.Utils.*

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GraknDAO {

    public final static String CREATE_PROJECT = Resources.toString(Resources.getResource(
            "queries/import/create_project.gql"), Charsets.UTF_8)
    public final static String CREATE_OPEN_SOURCE_FUNCTION = Resources.toString(Resources.getResource(
            "queries/import/create_open_source_function.gql"), Charsets.UTF_8)
    public final static String GET_OPEN_SOURCE_FUNCTION = Resources.toString(Resources.getResource(
            "queries/import/get_open_source_function.gql"), Charsets.UTF_8)
    public final static String GET_PROJECT = Resources.toString(Resources.getResource(
            "queries/get_project.gql"), Charsets.UTF_8)
    public final static String GET_PROJECT_EXTERNAL_METHOD_REFERENCE_COUNT = Resources.toString(Resources.getResource(
            "queries/get_project_external_method_reference_count.gql"), Charsets.UTF_8)
    public final static String GET_METHOD_EXTERNAL_METHOD_REFERENCE_COUNT = Resources.toString(Resources.getResource(
            "queries/get_method_external_method_reference_count.gql"), Charsets.UTF_8)
    public final static String GET_METHOD_EXTERNAL_METHOD_REFERENCES = Resources.toString(Resources.getResource(
            "queries/get_method_external_method_references.gql"), Charsets.UTF_8)
    public final static String GET_PROJECT_EXTERNAL_METHOD_REFERENCES = Resources.toString(Resources.getResource(
            "queries/get_project_external_method_references.gql"), Charsets.UTF_8)
    private final Vertx vertx
    private final RedisDAO redis
    private final GraknSession session

    GraknDAO(Vertx vertx, RedisDAO redis, GraknSession graknSession) {
        this.vertx = vertx
        this.redis = redis
        this.session = graknSession
    }

    OpenSourceFunction getOrCreateOpenSourceFunction(String functionName, QueryBuilder graql,
                                                     Handler<AsyncResult> handler) {
        boolean created = false
        def query = graql.parse(GET_OPEN_SOURCE_FUNCTION
                .replace("<name>", functionName))
        def match = query.execute() as List<QueryAnswer>
        if (match.isEmpty()) {
            match = graql.parse(CREATE_OPEN_SOURCE_FUNCTION
                    .replace("<name>", functionName)).execute() as List<QueryAnswer>
            created = true
        }
        def functionId = match.get(0).get("func").asEntity().id.toString()
        def functionDefinitionsId = match.get(0).get("funcDefs").asEntity().id.toString()
        def functionReferencesId = match.get(0).get("funcRefs").asEntity().id.toString()
        def osFunc = new OpenSourceFunction(functionId, functionDefinitionsId, functionReferencesId)
        if (created) {
            redis.cacheOpenSourceFunction(functionName, osFunc, handler)
            WebLauncher.metrics.counter("ImportMethod").inc()
        } else {
            handler.handle(Future.succeededFuture())
        }
        return osFunc
    }

    void getMethodMethodReferenceCount(String githubRepo, JsonArray methods, Handler<AsyncResult<JsonArray>> handler) {
        vertx.executeBlocking({ future ->
            def tx = null
            try {
                tx = session.open(GraknTxType.READ)
                def graql = tx.graql()
                def referenceCounts = new ArrayList<Long>()

                for (int i = 0; i < methods.size(); i++) {
                    def method = methods.getJsonObject(i)
                    def query = graql.parse(GET_METHOD_EXTERNAL_METHOD_REFERENCE_COUNT
                            .replace("<id>", method.getString("id")))
                    def res = (query.execute() as long)
                    referenceCounts.add(res)
                }
                future.complete(referenceCounts)
            } catch (all) {
                future.fail(all)
            } finally {
                tx?.close()
            }
        }, false, { res ->
            if (res.succeeded()) {
                def result = new JsonArray(res.result() as List)
                def futures = new ArrayList<Future>()
                for (int i = 0; i < result.size(); i++) {
                    def referenceCount = result.getLong(i)
                    if (referenceCount > 0) {
                        def cacheFuture = Future.future()
                        futures.add(cacheFuture)
                        redis.cacheMethodMethodReferenceCount(githubRepo, methods.getJsonObject(i),
                                referenceCount, cacheFuture.completer())
                    }
                }
                CompositeFuture.all(futures).setHandler({
                    if (it.succeeded()) {
                        handler.handle(Future.succeededFuture(result))
                    } else {
                        handler.handle(Future.failedFuture(it.cause()))
                    }
                })
            } else {
                handler.handle(Future.failedFuture(res.cause()))
            }
        })
    }

    void getProjectExternalMethodReferenceCount(String githubRepo, Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({ future ->
            def tx = null
            try {
                tx = session.open(GraknTxType.READ)
                def graql = tx.graql()
                def query = graql.parse(GET_PROJECT_EXTERNAL_METHOD_REFERENCE_COUNT
                        .replace("<githubRepo>", githubRepo))
                future.complete(query.execute())
            } catch (all) {
                future.fail(all)
            } finally {
                tx?.close()
            }
        }, false, { res ->
            if (res.succeeded()) {
                def result = res.result() as long
                redis.updateProjectReferenceLeaderboard(githubRepo, result, {
                    handler.handle(Future.succeededFuture(result))
                })
            } else {
                handler.handle(Future.failedFuture(res.cause()))
            }
        })
    }

    void getProjectMostExternalReferencedMethods(String githubRepo, Handler<AsyncResult<JsonArray>> handler) {
        getProjectExternalMethodReferences(githubRepo, { methods ->
            getMethodMethodReferenceCount(githubRepo, methods.result(), {
                redis.getProjectMostExternalReferencedMethods(githubRepo, 10, {
                    def futures = new ArrayList<Future>()
                    for (int i = 0; i < it.result().size(); i++) {
                        def ob = it.result().getJsonObject(i)
                        def future = Future.future()
                        futures.add(future)
                        getMethodExternalMethodReferences(githubRepo, ob.getString("id"), 0, future.completer())
                    }

                    CompositeFuture.all(futures).setHandler({ all ->
                        if (it.succeeded()) {
                            handler.handle(Future.succeededFuture(it.result()))
                        } else {
                            handler.handle(Future.failedFuture(all.cause()))
                        }
                    })
                })
            })
        })
    }

    void getMethodExternalMethodReferences(String githubRepo, String methodId, int offset,
                                           Handler<AsyncResult<JsonArray>> handler) {
        println "getMethodExternalMethodReferences: $githubRepo"
        vertx.executeBlocking({ future ->
            def tx = null
            try {
                tx = session.open(GraknTxType.READ)
                def graql = tx.graql()
                def query = graql.parse(GET_METHOD_EXTERNAL_METHOD_REFERENCES
                        .replace("<id>", methodId)
                        .replace("<githubRepo>", githubRepo)
                        .replace("offset 000", "offset " + offset))

                def rtnArray = new JsonArray()
                def result = query.execute() as List<QueryAnswer>
                for (def answer : result) {
                    def fileLocation = answer.get("file_location").asAttribute().getValue() as String
                    def commitSha1 = answer.get("commit_sha1").asAttribute().getValue() as String
                    def qualifiedName = answer.get("fu_name").asAttribute().getValue() as String
                    def functionId = answer.get("fu_ref").asEntity().id.value
                    def projectName = answer.get("p_name").asAttribute().value
                    def function = new JsonObject()
                            .put("qualified_name", qualifiedName)
                            .put("file_location", fileLocation)
                            .put("commit_sha1", commitSha1)
                            .put("id", functionId)
                            .put("short_class_name", getShortQualifiedClassName(qualifiedName))
                            .put("class_name", getQualifiedClassName(qualifiedName))
                            .put("short_method_signature", getShortMethodSignature(qualifiedName))
                            .put("method_signature", getMethodSignature(qualifiedName))
                            .put("github_repo", projectName)

                    rtnArray.add(function)
                }
                future.complete(rtnArray)
            } catch (all) {
                future.fail(all)
            } finally {
                tx?.close()
            }
        }, false, { res ->
            if (res.succeeded()) {
                def rtnArray = res.result() as JsonArray
                redis.cacheMethodMethodReferences(githubRepo, methodId, offset, rtnArray, {
                    handler.handle(Future.succeededFuture(rtnArray))
                })
            } else {
                handler.handle(Future.failedFuture(res.cause()))
            }
        })
    }

    void getProjectExternalMethodReferences(String githubRepo, Handler<AsyncResult<JsonArray>> handler) {
        vertx.executeBlocking({ future ->
            def tx = null
            try {
                tx = session.open(GraknTxType.READ)
                def graql = tx.graql()
                def query = graql.parse(GET_PROJECT_EXTERNAL_METHOD_REFERENCES.replace("<githubRepo>", githubRepo))

                def rtnArray = new JsonArray()
                def result = query.execute() as List<QueryAnswer>
                for (def answer : result) {
                    def fileLocation = answer.get("file_location").asAttribute().getValue() as String
                    def commitSha1 = answer.get("commit_sha1").asAttribute().getValue() as String
                    def qualifiedName = answer.get("fu_name").asAttribute().getValue() as String
                    def functionId = answer.get("real_fu").asEntity().id.value
                    def function = new JsonObject()
                            .put("qualified_name", qualifiedName)
                            .put("file_location", fileLocation)
                            .put("commit_sha1", commitSha1)
                            .put("id", functionId)
                            .put("short_class_name", getShortQualifiedClassName(qualifiedName))
                            .put("class_name", getQualifiedClassName(qualifiedName))
                            .put("short_method_signature", getShortMethodSignature(qualifiedName))
                            .put("method_signature", getMethodSignature(qualifiedName))
                            .put("github_repo", githubRepo.toLowerCase())
                    rtnArray.add(function)
                }
                future.complete(rtnArray)
            } catch (all) {
                future.fail(all)
            } finally {
                tx?.close()
            }
        }, false, handler)
    }

}
