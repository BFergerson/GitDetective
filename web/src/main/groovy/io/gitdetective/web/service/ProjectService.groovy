package io.gitdetective.web.service

import grakn.client.GraknClient
import groovy.util.logging.Slf4j
import io.gitdetective.web.WebServices
import io.gitdetective.web.dao.PostgresDAO
import io.gitdetective.web.service.model.FunctionReferenceInformation
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonObject

import static graql.lang.Graql.*

@Slf4j
class ProjectService extends AbstractVerticle {

    private final GraknClient.Session session
    private final PostgresDAO postgres

    ProjectService(GraknClient.Session session, PostgresDAO postgres) {
        this.session = Objects.requireNonNull(session)
        this.postgres = postgres //todo: require non-null
    }

    @Override
    void start() throws Exception {
        vertx.eventBus().consumer(WebServices.GET_FUNCTION_EXTERNAL_REFERENCES, { request ->
            def body = request.body() as JsonObject
            postgres.getFunctionReferences(body.getString("function_id"), 10, {
                if (it.succeeded()) {
                    println "here"
                } else {
                    request.fail(500, it.cause().message)
                }
            })
        })
    }

    void getOrCreateProject(String userId, String projectName, Handler<AsyncResult<String>> handler) {
        vertx.executeBlocking({
            try (def writeTx = session.transaction().write()) {
                def getProjectAnswer = writeTx.execute(match(
                        var("p").isa("project")
                                .has("project_name", projectName)
                ).get("p"))

                if (getProjectAnswer.isEmpty()) {
                    def createProjectAnswer = writeTx.execute(insert(
                            var("u").isa("user").id(userId),
                            var("p").isa("project")
                                    .has("project_name", projectName),
                            var().rel("has_project", var("u"))
                                    .rel("is_project", var("p")).isa("owns_project")
                    ))
                    handler.handle(Future.succeededFuture(createProjectAnswer.get(0).get("p").asEntity().id().value))
                } else {
                    handler.handle(Future.succeededFuture(getProjectAnswer.get(0).get("p").asEntity().id().value))
                }
            }
        }, false, handler)
    }

    void getProjectId(String projectName, Handler<AsyncResult<String>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def projectIdAnswer = readTx.execute(match(
                        var("p").isa("project")
                                .has("name", projectName)
                ).get("p"))

                if (projectIdAnswer.isEmpty()) {
                    handler.handle(Future.succeededFuture())
                } else {
                    handler.handle(Future.succeededFuture(projectIdAnswer.get(0).get("p").asEntity().id().value))
                }
            }
        }, false, handler)
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
