package io.gitdetective.web.dao

import ai.grakn.GraknSession
import ai.grakn.GraknTxType
import ai.grakn.concept.ConceptId
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

import java.time.Instant
import java.util.concurrent.TimeUnit

import static ai.grakn.graql.Graql.var
import static io.gitdetective.web.WebServices.*

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
            log.trace "Returned cached open source function"
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
                    .replace("<name>", functionName)
                    .replace("<createDate>", Instant.now().toString())).execute() as List<QueryAnswer>
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
            log.trace "Created open source function: $functionName"
        } else {
            handler.handle(Future.succeededFuture())
        }
        return osfFunction
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
                            .replace("<calcRefRounds>", method.getLong("calculated_reference_rounds") as String)
                            .replace("<id>", method.getString("id"))).execute() as List<QueryAnswer>

                    def methodRefs = new JsonArray()
                    for (def answer : result) {
                        def fileLocation = answer.get("file_location").asAttribute().getValue() as String
                        def commitSha1 = answer.get("commit_sha1").asAttribute().getValue() as String
                        def qualifiedName = answer.get("fu_name").asAttribute().getValue() as String
                        def fileOrFunctionId = answer.get("fu_ref").asEntity().id.value
                        def githubRepository = answer.get("p_name").asAttribute().value
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
                                    .put("github_repository", githubRepository)
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
                                    .put("github_repository", githubRepository)
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
