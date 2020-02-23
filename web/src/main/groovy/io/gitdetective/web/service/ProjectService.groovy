package io.gitdetective.web.service

import com.codebrig.arthur.SourceLanguage
import grakn.client.GraknClient
import graql.lang.statement.Statement
import groovy.util.logging.Slf4j
import io.gitdetective.web.WebServices
import io.gitdetective.web.dao.PostgresDAO
import io.gitdetective.web.model.FunctionInformation
import io.gitdetective.web.model.FunctionReference
import io.gitdetective.web.model.FunctionReferenceInformation
import io.vertx.core.*
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

import java.time.Instant
import java.time.LocalDateTime
import java.util.stream.Collectors

import static graql.lang.Graql.*

@Slf4j
class ProjectService extends AbstractVerticle {

    private final GraknClient.Session session
    private final PostgresDAO postgres

    ProjectService(GraknClient.Session session, PostgresDAO postgres) {
        this.session = Objects.requireNonNull(session)
        this.postgres = Objects.requireNonNull(postgres)
    }

    @Override
    void start() throws Exception {
        vertx.eventBus().consumer(WebServices.GET_FUNCTION_EXTERNAL_REFERENCES, { request ->
            def body = request.body() as JsonObject
            postgres.getFunctionReferences(body.getString("function_id"), 10, {
                if (it.succeeded()) {
                    getFunctionReferences(it.result(), 10, {
                        if (it.succeeded()) {
                            request.reply(new JsonArray(Json.encode(it.result())))
                        } else {
                            request.fail(500, it.cause().message)
                        }
                    })
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
                                    .has("create_date", LocalDateTime.now())
                                    .has("reference_count", 0),
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
                                .rel("is_defines_function", var("f")).isa("defines_function")
                ).get("f").count())

                handler.handle(Future.succeededFuture(functionCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getFunctionIds(String projectIdOrName, Handler<AsyncResult<List<String>>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def projectIdOrNameVar
                if (projectIdOrName.contains("/")) {
                    projectIdOrNameVar = var("p").has("project_name", projectIdOrName)
                } else {
                    projectIdOrNameVar = var("p").id(projectIdOrName)
                }
                def functionIdsAnswer = readTx.execute(match(
                        projectIdOrNameVar,
                        var("fi").isa("file"),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file"),
                        var("f").isa("function"),
                        var().rel("has_defines_function", var("fi"))
                                .rel("is_defines_function", var("f")).isa("defines_function")
                ).get("f"))

                handler.handle(Future.succeededFuture(
                        functionIdsAnswer.stream().map({ it.get("f").asEntity().id().value })
                                .collect(Collectors.toList())
                ))
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

    void getFunctionReferences(List<FunctionReference> partialFunctionReferences, int limit,
                               Handler<AsyncResult<List<FunctionReference>>> handler) {
        def functionMap = new HashMap<String, FunctionReference>()
        partialFunctionReferences.each {
            functionMap.put(it.functionId, it)
        }
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def callerFunctionOrs = new ArrayList<Statement>()
                def callerProjectOrs = new ArrayList<Statement>()
                partialFunctionReferences.each {
                    callerFunctionOrs << var("f").id(it.functionId)
                    callerProjectOrs << var("p_ref").id(it.projectId)
                }

                def functionReferencesAnswer = readTx.execute(match(
                        or(callerFunctionOrs),
                        var("f_ref").isa("function")
                                .has("kythe_uri", var("k_uri"))
                                .has("qualified_name", var("q_name")),
                        var().rel("has_reference_call", var("f_ref"))
                                .rel("is_reference_call", var("f")).isa("reference_call"),
                        var("fi_ref").isa("file")
                                .has("file_location", var("f_loc")),
                        var().rel("has_defines_function", var("fi_ref"))
                                .rel("is_defines_function", var("f_ref")).isa("defines_function"),
                        or(callerProjectOrs),
                        var("p_ref")
                                .has("project_name", var("p_name")),
                        var().rel("has_defines_file", var("p_ref"))
                                .rel("is_defines_file", var("fi_ref")).isa("defines_file")
                ).get("f", "k_uri", "q_name", "f_loc", "p_name").limit(limit))

                def result = []
                functionReferencesAnswer.each {
                    def functionId = it.get("f").asEntity().id().value
                    def ref = functionMap.get(functionId)
                    ref.kytheUri = it.get("k_uri").asAttribute().value() as String
                    ref.qualifiedName = it.get("q_name").asAttribute().value() as String
                    ref.fileLocation = it.get("f_loc").asAttribute().value() as String
                    ref.projectName = it.get("p_name").asAttribute().value() as String
                    result << ref
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
                                    .has("language", SourceLanguage.getSourceLanguage(fileLocation).key)
                                    .has("file_location", fileLocation)
                                    .has("reference_count", 0),
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

    void getOrCreateFiles(List<Tuple3<String, String, String>> fileData,
                          Handler<AsyncResult<List<String>>> handler) {
        vertx.executeBlocking({
            try (def writeTx = session.transaction().write()) {
                def fileIds = new ArrayList<String>()
                boolean wroteData = false
                fileData.each {
                    def getFileAnswer = writeTx.execute(match(
                            var("p").id(it.v1),
                            var("fi").isa("file")
                                    .has("qualified_name", it.v2),
                            var().rel("has_defines_file", var("p"))
                                    .rel("is_defines_file", var("fi")).isa("defines_file")
                    ).get("fi"))

                    if (getFileAnswer.isEmpty()) {
                        def createFileAnswer = writeTx.execute(insert(
                                var("p").id(it.v1),
                                var("fi").isa("file")
                                        .has("qualified_name", it.v2)
                                        .has("language", SourceLanguage.getSourceLanguage(it.v3).key)
                                        .has("file_location", it.v3)
                                        .has("reference_count", 0),
                                var().rel("has_defines_file", var("p"))
                                        .rel("is_defines_file", var("fi")).isa("defines_file")
                        ))
                        wroteData = true
                        fileIds << createFileAnswer.get(0).get("fi").asEntity().id().value
                    } else {
                        fileIds << getFileAnswer.get(0).get("fi").asEntity().id().value
                    }
                }
                if (wroteData) {
                    writeTx.commit()
                }
                handler.handle(Future.succeededFuture(fileIds))
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
        log.info("getOrCreateFunction - File id: $fileId - Function: $function - Reference count: $referenceCount")
        vertx.executeBlocking({
            try (def writeTx = session.transaction().write()) {
                def getFunctionAnswer = writeTx.execute(match(
                        var("f").isa("function")
                                .has("kythe_uri", function.kytheUri)
                                .has("qualified_name", var("q_name"), var("q_name_rel"))
                ).get("f", "q_name", "q_name_rel"))

                if (getFunctionAnswer.isEmpty()) {
                    log.info("createFunction - File id: $fileId - Function: $function - Reference count: $referenceCount")
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
                        createFunctionAnswer = writeTx.execute(insert(
                                var("f").isa("function")
                                        .has("kythe_uri", function.kytheUri)
                                        .has("qualified_name", function.qualifiedName)
                                        .has("reference_count", referenceCount)
                        ))
                    }
                    writeTx.commit()
                    handler.handle(Future.succeededFuture(createFunctionAnswer.get(0).get("f").asEntity().id().value))
                } else {
                    log.info("getFunction - File id: $fileId - Function: $function - Reference count: $referenceCount")
                    if (fileId != null) {
                        //function definer; ensure qualified name is correct
                        def wroteData = false
                        def currentQualifiedName = getFunctionAnswer.get(0).get("q_name").asAttribute().value() as String
                        def qualifiedNameRelId = getFunctionAnswer.get(0).get("q_name_rel").asRelation().id().value
                        def functionId = getFunctionAnswer.get(0).get("f").asEntity().id().value
                        if (currentQualifiedName != function.qualifiedName) {
                            writeTx.execute(parse(
                                    'match $r id ' + qualifiedNameRelId + '; delete $r;'
                            ))
                            writeTx.execute(match(
                                    var("f").id(functionId)).insert(
                                    var("f").has("qualified_name", function.qualifiedName)
                            ))
                            wroteData = true
                        }

                        //function definer; ensure function is associated to file
                        def getFunctionFileRelationAnswer = writeTx.execute(match(
                                var("f").id(functionId),
                                var("fi").id(fileId),
                                var().rel("has_defines_function", var("fi"))
                                        .rel("is_defines_function", var("f")).isa("defines_function")
                        ).get())
                        if (getFunctionFileRelationAnswer.isEmpty()) {
                            writeTx.execute(insert(
                                    var("f").id(functionId),
                                    var("fi").id(fileId),
                                    var().rel("has_defines_function", var("fi"))
                                            .rel("is_defines_function", var("f")).isa("defines_function")
                            ))
                            wroteData = true
                        }

                        if (wroteData) {
                            writeTx.commit()
                        }
                    }
                    handler.handle(Future.succeededFuture(getFunctionAnswer.get(0).get("f").asEntity().id().value))
                }
            } catch (all) {
                handler.handle(Future.failedFuture(all))
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

    void insertFunctionReferences(List<Tuple7<String, String, Instant, String, Integer, FunctionInformation, FunctionInformation>> referenceData,
                                  Handler<AsyncResult<Void>> handler) {
        def futures = new ArrayList<Future>()
        referenceData.each {
            def projectId = it.v1
            def callerCommitDate = it.v3
            def callerCommitSha1 = it.v4
            def callerLineNumber = it.v5
            def callerFunctionFut = Future.future()
            getOrCreateFunction(it.v2, it.v6, 0, callerFunctionFut)
            def calleeFunctionFut = Future.future()
            getOrCreateFunction(it.v7, 1, calleeFunctionFut)

            def future = Future.future()
            futures << future
            CompositeFuture.all(callerFunctionFut, calleeFunctionFut).setHandler({
                if (it.succeeded()) {
                    def callerFunctionId = it.result().list().get(0) as String
                    def calleeFunctionId = it.result().list().get(1) as String
                    getOrCreateFunctionReference(callerFunctionId, calleeFunctionId, {
                        if (it.succeeded()) {
                            postgres.insertFunctionReference(projectId, calleeFunctionId, calleeFunctionId,
                                    callerCommitSha1, callerCommitDate, callerLineNumber, future)
                        } else {
                            future.fail(it.cause())
                        }
                    })
                } else {
                    future.fail(it.cause())
                }
            })
        }

        CompositeFuture.all(futures).setHandler({
            if (it.succeeded()) {
                handler.handle(Future.succeededFuture())
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
