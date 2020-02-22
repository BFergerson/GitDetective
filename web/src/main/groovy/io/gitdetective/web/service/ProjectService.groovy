package io.gitdetective.web.service

import grakn.client.GraknClient
import groovy.util.logging.Slf4j
import io.gitdetective.web.WebServices
import io.gitdetective.web.dao.PostgresDAO
import io.gitdetective.web.model.FunctionInformation
import io.gitdetective.web.model.FunctionReferenceInformation
import io.vertx.core.*
import io.vertx.core.json.JsonObject

import java.time.Instant
import java.time.LocalDateTime

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
                            var("u").id(userId),
                            var("p").isa("project")
                                    .has("project_name", projectName)
                                    .has("create_date", LocalDateTime.now()),
                            var().rel("has_project", var("u"))
                                    .rel("is_project", var("p")).isa("owns_project")
                    ))
                    writeTx.commit()
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

    void getOrCreateFile(String projectId, String qualifiedName, String fileLocation,
                         Handler<AsyncResult<String>> handler) {
        vertx.executeBlocking({
            try (def writeTx = session.transaction().write()) {
                def getFileAnswer = writeTx.execute(match(
                        var("p").id(projectId),
                        var("fi").isa("file")
                                .has("qualified_name", qualifiedName),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file")
                ).get("fi"))

                if (getFileAnswer.isEmpty()) {
                    def createFileAnswer = writeTx.execute(insert(
                            var("p").id(projectId),
                            var("fi").isa("file")
                                    .has("qualified_name", qualifiedName)
                                    .has("file_location", fileLocation),
                            var().rel("has_defines_file", var("p"))
                                    .rel("is_defines_file", var("fi")).isa("defines_file")
                    ))
                    writeTx.commit()
                    handler.handle(Future.succeededFuture(createFileAnswer.get(0).get("fi").asEntity().id().value))
                } else {
                    handler.handle(Future.succeededFuture(getFileAnswer.get(0).get("fi").asEntity().id().value))
                }
            }
        }, false, handler)
    }

    void getOrCreateFunction(FunctionInformation function, int referenceCount,
                             Handler<AsyncResult<String>> handler) {
        getOrCreateFunction(null, function, referenceCount, handler)
    }

    void getOrCreateFunction(String fileId, FunctionInformation function,
                             Handler<AsyncResult<String>> handler) {
        getOrCreateFunction(fileId, function, 0, handler)
    }

    void getOrCreateFunction(String fileId, FunctionInformation function, int referenceCount,
                             Handler<AsyncResult<String>> handler) {
        vertx.executeBlocking({
            try (def writeTx = session.transaction().write()) {
                def getFunctionAnswer = writeTx.execute(match(
                        var("f").isa("function")
                                .has("kythe_uri", function.kytheUri)
                ).get("f"))

                if (getFunctionAnswer.isEmpty()) {
                    def createFunctionAnswer
                    if (fileId != null) {
                        createFunctionAnswer = writeTx.execute(insert(
                                var("fi").id(fileId),
                                var("f").isa("function")
                                        .has("kythe_uri", function.kytheUri)
                                        .has("qualified_name", function.qualifiedName)
                                        .has("reference_count", referenceCount),
                                var().rel("has_defines_function", var("fi"))
                                        .rel("is_defines_function", var("f")).isa("defines_function")
                        ))
                    } else {
                        //if they don't know file then they don't know real qualified name
                        createFunctionAnswer = writeTx.execute(insert(
                                var("f").isa("function")
                                        .has("kythe_uri", function.kytheUri)
                                        .has("reference_count", referenceCount)
                        ))
                    }
                    writeTx.commit()
                    handler.handle(Future.succeededFuture(createFunctionAnswer.get(0).get("f").asEntity().id().value))
                } else {
                    handler.handle(Future.succeededFuture(getFunctionAnswer.get(0).get("f").asEntity().id().value))
                }
            }
        }, false, handler)
    }

    void insertFunctionReference(String projectId, String callerFileId,
                                 Instant callerCommitDate, String callerCommitSha1, int callerLineNumber,
                                 FunctionInformation callerFunction, FunctionInformation calleeFunction,
                                 Handler<AsyncResult<Void>> handler) {
        def callerFunctionFut = Future.future()
        getOrCreateFunction(callerFileId, callerFunction, 0, callerFunctionFut)
        def calleeFunctionFut = Future.future()
        getOrCreateFunction(calleeFunction, 1, calleeFunctionFut)
        CompositeFuture.all(callerFunctionFut, calleeFunctionFut).setHandler({
            if (it.succeeded()) {
                def callerFunctionId = it.result().list().get(0) as String
                def calleeFunctionId = it.result().list().get(1) as String
                getOrCreateFunctionReference(callerFunctionId, calleeFunctionId, {
                    if (it.succeeded()) {
                        postgres.insertFunctionReference(projectId, calleeFunctionId, calleeFunctionId,
                                callerCommitSha1, callerCommitDate, callerLineNumber, {
                            if (it.succeeded()) {
                                handler.handle(Future.succeededFuture())
                            } else {
                                handler.handle(Future.failedFuture(it.cause()))
                            }
                        })
                    } else {
                        handler.handle(Future.failedFuture(it.cause()))
                    }
                })
            } else {
                handler.handle(Future.failedFuture(it.cause()))
            }
        })
    }

    private void getOrCreateFunctionReference(String callerFunctionId, String calleeFunctionId,
                                              Handler<AsyncResult<String>> handler) {
        vertx.executeBlocking({
            try (def writeTx = session.transaction().write()) {
                def getReferenceAnswer = writeTx.execute(match(
                        var("f1").id(callerFunctionId),
                        var("f2").id(calleeFunctionId),
                        var("ref").rel("has_reference_call", var("f1"))
                                .rel("is_reference_call", var("f2")).isa("reference_call")
                ).get("ref"))

                if (getReferenceAnswer.isEmpty()) {
                    def createReferenceAnswer = writeTx.execute(insert(
                            var("f1").id(callerFunctionId),
                            var("f2").id(calleeFunctionId),
                            var("ref").rel("has_reference_call", var("f1"))
                                    .rel("is_reference_call", var("f2")).isa("reference_call")
                    ))
                    writeTx.commit()
                    handler.handle(Future.succeededFuture(createReferenceAnswer.get(0).get("ref").asRelation().id().value))
                } else {
                    handler.handle(Future.succeededFuture(getReferenceAnswer.get(0).get("ref").asRelation().id().value))
                }
            }
        }, false, handler)
    }
}
