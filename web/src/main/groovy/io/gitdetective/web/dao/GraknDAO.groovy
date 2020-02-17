//package io.gitdetective.web.dao
//
//import ai.grakn.GraknSession
//import ai.grakn.GraknTxType
//import ai.grakn.graql.QueryBuilder
//import ai.grakn.graql.answer.ConceptMap
//import com.google.common.base.Charsets
//import com.google.common.cache.Cache
//import com.google.common.cache.CacheBuilder
//import com.google.common.io.Resources
//import io.gitdetective.web.WebLauncher
//import io.gitdetective.web.work.importer.OpenSourceFunction
//import io.vertx.core.AsyncResult
//import io.vertx.core.Future
//import io.vertx.core.Handler
//import io.vertx.core.Vertx
//import io.vertx.core.logging.Logger
//import io.vertx.core.logging.LoggerFactory
//
//import java.time.Instant
//import java.util.concurrent.TimeUnit
//
///**
// * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
// */
//class GraknDAO {
//
//    public final static String CREATE_PROJECT = Resources.toString(Resources.getResource(
//            "queries/import/create_project.gql"), Charsets.UTF_8)
//    public final static String CREATE_OPEN_SOURCE_FUNCTION = Resources.toString(Resources.getResource(
//            "queries/import/create_open_source_function.gql"), Charsets.UTF_8)
//    public final static String GET_OPEN_SOURCE_FUNCTION = Resources.toString(Resources.getResource(
//            "queries/import/get_open_source_function.gql"), Charsets.UTF_8)
//    public final static String GET_PROJECT = Resources.toString(Resources.getResource(
//            "queries/get_project.gql"), Charsets.UTF_8)
//    public final static String GET_FUNCTION_QUALIFIED_NAME = Resources.toString(Resources.getResource(
//            "queries/get_function_qualified_name.gql"), Charsets.UTF_8)
//    private final static Logger log = LoggerFactory.getLogger(GraknDAO.class)
//    private final Vertx vertx
//    private final RedisDAO redis
//    private final GraknSession session
//    private final Cache<String, OpenSourceFunction> osfCache = CacheBuilder.newBuilder()
//            .expireAfterWrite(1, TimeUnit.MINUTES).build()
//
//    GraknDAO(Vertx vertx, RedisDAO redis, GraknSession graknSession) {
//        this.vertx = vertx
//        this.redis = redis
//        this.session = graknSession
//    }
//
//    OpenSourceFunction getOrCreateOpenSourceFunction(String functionName, QueryBuilder graql,
//                                                     Handler<AsyncResult> handler) {
//        def osfFunction = osfCache.getIfPresent(functionName)
//        if (osfFunction != null) {
//            log.trace "Returned cached open source function"
//            handler.handle(Future.succeededFuture())
//            return osfFunction
//        } else {
//            osfFunction = new OpenSourceFunction()
//            osfCache.put(functionName, osfFunction)
//        }
//
//        boolean created = false
//        def query = graql.parse(GET_OPEN_SOURCE_FUNCTION
//                .replace("<name>", functionName))
//        def match = query.execute() as List<ConceptMap>
//        if (match.isEmpty()) {
//            match = graql.parse(CREATE_OPEN_SOURCE_FUNCTION
//                    .replace("<name>", functionName)
//                    .replace("<createDate>", Instant.now().toString())).execute() as List<ConceptMap>
//            created = true
//        }
//        def functionId = match.get(0).get("func").asEntity().id.toString()
//        def functionDefinitionsId = match.get(0).get("funcDefs").asEntity().id.toString()
//        def functionReferencesId = match.get(0).get("funcRefs").asEntity().id.toString()
//        osfFunction.functionId = functionId
//        osfFunction.functionDefinitionsId = functionDefinitionsId
//        osfFunction.functionReferencesId = functionReferencesId
//        if (created) {
//            redis.cacheOpenSourceFunction(functionName, osfFunction, handler)
//            WebLauncher.metrics.counter("ImportMethod").inc()
//            log.trace "Created open source function: $functionName"
//        } else {
//            handler.handle(Future.succeededFuture())
//        }
//        return osfFunction
//    }
//
//    void getFunctionQualifiedName(String functionId, Handler<AsyncResult<String>> handler) {
//        vertx.executeBlocking({ blocking ->
//            def tx = null
//            try {
//                tx = session.transaction(GraknTxType.READ)
//                def graql = tx.graql()
//                def query = graql.parse(GET_FUNCTION_QUALIFIED_NAME
//                        .replace("<id>", functionId))
//
//                def result = query.execute() as List<ConceptMap>
//                if (!result.isEmpty()) {
//                    blocking.complete(result.get(0).get("qualifiedName").asAttribute().value() as String)
//                } else {
//                    blocking.complete()
//                }
//            } catch (all) {
//                blocking.fail(all)
//            } finally {
//                tx?.close()
//            }
//        }, false, handler)
//    }
//
//
//}
