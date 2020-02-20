package io.gitdetective.web.service

import grakn.client.GraknClient
import groovy.util.logging.Slf4j
import io.gitdetective.web.service.model.FunctionReferenceInformation
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import static graql.lang.Graql.match
import static graql.lang.Graql.var

@Slf4j
class ProjectService extends AbstractVerticle {

    private final GraknClient.Session session

    ProjectService(GraknClient.Session session) {
        this.session = Objects.requireNonNull(session)
    }

    @Override
    void start() throws Exception {
//        vertx.eventBus().consumer(GET_PROJECT_FILE_COUNT, { request ->
//            def timer = WebLauncher.metrics.timer(GET_PROJECT_FILE_COUNT)
//            def context = timer.time()
//            def body = (JsonObject) request.body()
//            def githubRepository = body.getString("github_repository").toLowerCase()
//            log.debug "Getting project file count: " + githubRepository
//
//            redis.getProjectFileCount(githubRepository, {
//                if (it.failed()) {
//                    it.cause().printStackTrace()
//                    request.reply(it.cause())
//                } else {
//                    log.debug "Got file count: " + it.result() + " - Repo: " + githubRepository
//                    request.reply(it.result())
//                }
//                context.stop()
//            })
//        })
//        vertx.eventBus().consumer(GET_PROJECT_METHOD_INSTANCE_COUNT, { request ->
//            def timer = WebLauncher.metrics.timer(GET_PROJECT_METHOD_INSTANCE_COUNT)
//            def context = timer.time()
//            def body = (JsonObject) request.body()
//            def githubRepository = body.getString("github_repository").toLowerCase()
//            log.debug "Getting project method instance count: " + githubRepository
//
//            redis.getProjectMethodInstanceCount(githubRepository, {
//                if (it.failed()) {
//                    it.cause().printStackTrace()
//                    request.reply(it.cause())
//                } else {
//                    log.debug "Got method instance count: " + it.result() + " - Repo: " + githubRepository
//                    request.reply(it.result())
//                }
//                context.stop()
//            })
//        })
//        vertx.eventBus().consumer(GET_PROJECT_FIRST_INDEXED, { request ->
//            def timer = WebLauncher.metrics.timer(GET_PROJECT_FIRST_INDEXED)
//            def context = timer.time()
//            def body = (JsonObject) request.body()
//            def githubRepository = body.getString("github_repository").toLowerCase()
//            log.debug "Getting project first indexed"
//
//            redis.getProjectFirstIndexed(githubRepository, {
//                if (it.failed()) {
//                    it.cause().printStackTrace()
//                    request.reply(it.cause())
//                } else {
//                    log.debug "Got project first indexed: " + it.result()
//                    request.reply(it.result())
//                }
//                context.stop()
//            })
//        })
//        vertx.eventBus().consumer(GET_PROJECT_LAST_INDEXED, { request ->
//            def timer = WebLauncher.metrics.timer(GET_PROJECT_LAST_INDEXED)
//            def context = timer.time()
//            def body = (JsonObject) request.body()
//            def githubRepository = body.getString("github_repository").toLowerCase()
//            log.debug "Getting project last indexed"
//
//            redis.getProjectLastIndexed(githubRepository, {
//                if (it.failed()) {
//                    it.cause().printStackTrace()
//                    request.reply(it.cause())
//                } else {
//                    log.debug "Got project last indexed: " + it.result()
//                    request.reply(it.result())
//                }
//                context.stop()
//            })
//        })
//        vertx.eventBus().consumer(GET_PROJECT_LAST_INDEXED_COMMIT_INFORMATION, { request ->
//            def timer = WebLauncher.metrics.timer(GET_PROJECT_LAST_INDEXED_COMMIT_INFORMATION)
//            def context = timer.time()
//            def body = (JsonObject) request.body()
//            def githubRepository = body.getString("github_repository").toLowerCase()
//            log.debug "Getting project last indexed commit information"
//
//            redis.getProjectLastIndexedCommitInformation(githubRepository, {
//                if (it.failed()) {
//                    it.cause().printStackTrace()
//                    request.reply(it.cause())
//                } else {
//                    log.debug "Got project last indexed commit information: " + it.result()
//                    request.reply(it.result())
//                }
//                context.stop()
//            })
//        })
    }

    void getFileCount(String projectName, Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def fileCountAnswer = readTx.execute(match(
                        var("p").isa("project")
                                .has("name", projectName),
                        var("fi").isa("file"),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file")
                ).get("fi").count())

                handler.handle(Future.succeededFuture(fileCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getFunctionCount(String projectName, Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def functionCountAnswer = readTx.execute(match(
                        var("p").isa("project")
                                .has("name", projectName),
                        var("fi").isa("file"),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file"),
                        var("f").isa("function"),
                        var().rel("has_defines_function", var("fi"))
                                .rel("is_defines_function", var("f")).isa("defines_function"),
                ).get("f").count())

                handler.handle(Future.succeededFuture(functionCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getMostReferencedFunctionsInformation(String projectName, int limit,
                                               Handler<AsyncResult<List<FunctionReferenceInformation>>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def mostReferencedFunctionsAnswer = readTx.execute(match(
                        var("p").isa("project")
                                .has("name", projectName),
                        var("fi").isa("file"),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file"),
                        var("f").isa("function")
                                .has("kythe_uri", var("k_uri"))
                                .has("qualified_name", var("q_name"))
                                .has("reference_count", var("ref_count")),
                        var().rel("has_defines_function", var("fi"))
                                .rel("is_defines_function", var("f")).isa("defines_function"),
                ).get("f", "k_uri", "q_name", "ref_count").sort("ref_count", "desc").limit(limit))

                def result = []
                mostReferencedFunctionsAnswer.each {
                    def functionId = it.get("f").asEntity().id().value
                    def kytheUri = it.get("k_uri").asAttribute().value() as String
                    def qualifiedName = it.get("q_name").asAttribute().value() as String
                    def referenceCount = it.get("ref_count").asAttribute().value() as int
                    result << new FunctionReferenceInformation(functionId, kytheUri, qualifiedName, referenceCount)
                }
                handler.handle(Future.succeededFuture(result))
            }
        }, false, handler)
    }
}
