package io.gitdetective.web.dao

import ai.grakn.GraknSession
import ai.grakn.GraknTxType
import ai.grakn.graql.QueryBuilder
import ai.grakn.graql.internal.query.QueryAnswer
import com.google.common.base.Charsets
import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.google.common.io.Resources
import io.gitdetective.web.WebLauncher
import io.gitdetective.web.work.importer.OpenSourceFunction
import io.vertx.core.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory

import java.util.concurrent.TimeUnit

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
    public final static String GET_PROJECT_NEW_EXTERNAL_REFERENCES = Resources.toString(Resources.getResource(
            "queries/get_project_new_external_references.gql"), Charsets.UTF_8)
    public final static String GET_METHOD_NEW_EXTERNAL_REFERENCES = Resources.toString(Resources.getResource(
            "queries/get_method_new_external_references.gql"), Charsets.UTF_8)
    private final static Logger log = LoggerFactory.getLogger(GraknDAO.class)
    private final Vertx vertx
    private final RedisDAO redis
    private final GraknSession session
    private final Cache<String, OpenSourceFunction> osfCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES).build()

    GraknDAO(Vertx vertx, RedisDAO redis, GraknSession graknSession) {
        this.vertx = vertx
        this.redis = redis
        this.session = graknSession
    }

    OpenSourceFunction getOrCreateOpenSourceFunction(String functionName, QueryBuilder graql,
                                                     Handler<AsyncResult> handler) {
        def osfFunction = osfCache.getIfPresent(functionName)
        if (osfFunction != null) {
            log.trace("Returned cached open source function")
            handler.handle(Future.succeededFuture())
            return osfFunction
        } else {
            osfFunction = new OpenSourceFunction()
            osfCache.put(functionName, osfFunction)
        }

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
        osfFunction.functionId = functionId
        osfFunction.functionDefinitionsId = functionDefinitionsId
        osfFunction.functionReferencesId = functionReferencesId
        if (created) {
            redis.cacheOpenSourceFunction(functionName, osfFunction, handler)
            WebLauncher.metrics.counter("ImportMethod").inc()
            log.debug "Created open source function: $functionName"
        } else {
            handler.handle(Future.succeededFuture())
        }
        return osfFunction
    }

    void getProjectMostExternalReferencedMethods(String githubRepository, Handler<AsyncResult<JsonArray>> handler) {
        getProjectNewExternalReferences(githubRepository, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def referencedMethods = it.result()
                getMethodNewExternalReferences(referencedMethods, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        //update cache
                        def cacheFutures = new ArrayList<Future>()
                        def refMethods = it.result()
                        def totalRefCount = 0
                        for (int i = 0; i < refMethods.size(); i++) {
                            def method = referencedMethods.getJsonObject(i)
                            def methodRefs = refMethods.getJsonArray(i)
                            totalRefCount += methodRefs.size()

                            def fut = Future.future()
                            cacheFutures.add(fut)
                            redis.cacheMethodReferences(githubRepository, method, methodRefs, fut.completer())
                        }

                        CompositeFuture.all(cacheFutures).setHandler({
                            if (it.failed()) {
                                handler.handle(Future.failedFuture(it.cause()))
                            } else {
                                def res = it.result().list() as List<Long>
                                updateMethodDefinitionComputedInstanceOffsets(referencedMethods, res, {
                                    if (it.failed()) {
                                        handler.handle(Future.failedFuture(it.cause()))
                                    } else {
                                        //update project leaderboard
                                        redis.updateProjectReferenceLeaderboard(githubRepository, totalRefCount, {
                                            if (it.failed()) {
                                                handler.handle(Future.failedFuture(it.cause()))
                                            } else {
                                                //return from cache
                                                redis.getProjectMostExternalReferencedMethods(githubRepository, 10, handler)
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

    private void updateMethodDefinitionComputedInstanceOffsets(JsonArray methods, List<Long> computedInstanceOffsets,
                                                               Handler<AsyncResult> handler) {
        if (methods.isEmpty()) {
            handler.handle(Future.succeededFuture())
            return
        }
        log.info "Updating " + methods.size() + " method definition computed offsets"

        vertx.executeBlocking({ blocking ->
            def tx = null
            try {
                tx = session.open(GraknTxType.BATCH)
                def graql = tx.graql()
                for (int i = 0; i < methods.size(); i++) {
                    def method = methods.getJsonObject(i)
                    graql.parse(('match $x id "<id>"; ' +
                            '$defRel (has_defines_function: $file, is_defines_function: $x) isa defines_function; ' +
                            '$defRel has computed_instance_offset $compOffset; $offsetRel ($defRel, $compOffset); ' +
                            'delete $offsetRel;')
                            .replace("<id>", method.getString("id"))).execute() as List<QueryAnswer>
                    graql.parse(('match $x id "<id>"; ' +
                            '$defRel (has_defines_function: $file, is_defines_function: $x) isa defines_function; ' +
                            'insert $defRel has computed_instance_offset ' + computedInstanceOffsets.get(i) + ';')
                            .replace("<id>", method.getString("id"))).execute() as List<QueryAnswer>
                }
                tx.commit()
                blocking.complete(Future.succeededFuture())
            } catch (all) {
                blocking.fail(all)
            } finally {
                tx?.close()
            }
        }, false, handler)
    }

    void getProjectNewExternalReferences(String githubRepository, Handler<AsyncResult<JsonArray>> handler) {
        vertx.executeBlocking({ future ->
            def tx = null
            try {
                tx = session.open(GraknTxType.READ)
                def graql = tx.graql()
                def query = graql.parse(GET_PROJECT_NEW_EXTERNAL_REFERENCES
                        .replace("<githubRepo>", githubRepository))

                def rtnArray = new JsonArray()
                def result = query.execute() as List<QueryAnswer>
                for (def answer : result) {
                    def fileLocation = answer.get("file_location").asAttribute().getValue() as String
                    def commitSha1 = answer.get("commit_sha1").asAttribute().getValue() as String
                    def qualifiedName = answer.get("fu_name").asAttribute().getValue() as String
                    def osfId = answer.get("open_fu").asEntity().id.value
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
                            .put("github_repository", githubRepository.toLowerCase())
                            .put("osf_id", osfId)
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

    void getMethodNewExternalReferences(JsonArray methods, Handler<AsyncResult<JsonArray>> handler) {
        if (methods.isEmpty()) {
            handler.handle(Future.succeededFuture(new JsonArray()))
            return
        }

        vertx.executeBlocking({ blocking ->
            def tx = null
            def rtnArray = new JsonArray()
            try {
                tx = session.open(GraknTxType.BATCH)
                def graql = tx.graql()
                for (int i = 0; i < methods.size(); i++) {
                    def method = methods.getJsonObject(i)
                    def result = graql.parse(GET_METHOD_NEW_EXTERNAL_REFERENCES
                            .replace("<id>", method.getString("id"))).execute() as List<QueryAnswer>

                    def methodRefs = new JsonArray()
                    for (def answer : result) {
                        def fileLocation = answer.get("file_location").asAttribute().getValue() as String
                        def commitSha1 = answer.get("commit_sha1").asAttribute().getValue() as String
                        def qualifiedName = answer.get("fu_name").asAttribute().getValue() as String
                        def fileOrFunctionId = answer.get("fu_ref").asEntity().id.value
                        def projectName = answer.get("p_name").asAttribute().value
                        if (qualifiedName.contains("(")) {
                            //function ref
                            def function = new JsonObject()
                                    .put("qualified_name", qualifiedName)
                                    .put("file_location", fileLocation)
                                    .put("commit_sha1", commitSha1)
                                    .put("id", fileOrFunctionId)
                                    .put("short_class_name", getShortQualifiedClassName(qualifiedName))
                                    .put("class_name", getQualifiedClassName(qualifiedName))
                                    .put("short_method_signature", getShortMethodSignature(qualifiedName))
                                    .put("method_signature", getMethodSignature(qualifiedName))
                                    .put("github_repository", projectName)
                                    .put("is_function", true)
                            methodRefs.add(function)
                        } else {
                            //file ref
                            def file = new JsonObject()
                                    .put("qualified_name", qualifiedName)
                                    .put("file_location", fileLocation)
                                    .put("commit_sha1", commitSha1)
                                    .put("id", fileOrFunctionId)
                                    .put("short_class_name", getFilename(fileLocation))
                                    .put("github_repository", projectName)
                                    .put("is_file", true)
                            methodRefs.add(file)
                        }
                    }
                    rtnArray.add(methodRefs)
                }
                tx.commit()
                blocking.complete(rtnArray)
            } catch (all) {
                blocking.fail(all)
            } finally {
                tx?.close()
            }
        }, false, handler)
    }

}
