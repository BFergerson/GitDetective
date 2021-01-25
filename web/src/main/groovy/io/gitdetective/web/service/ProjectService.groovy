package io.gitdetective.web.service

import com.codebrig.arthur.SourceLanguage
import grakn.client.Grakn
import grakn.client.GraknClient
import grakn.client.concept.answer.ConceptMap
import grakn.client.rpc.RPCSession
import graql.lang.Graql
import graql.lang.pattern.variable.ThingVariable
import graql.lang.query.GraqlMatch
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
import java.time.temporal.ChronoUnit
import java.util.stream.Collectors

import static graql.lang.Graql.*

@Slf4j
class ProjectService extends AbstractVerticle {

    private final RPCSession.Core session
    private final PostgresDAO postgres

    ProjectService(RPCSession.Core session, PostgresDAO postgres) {
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
            try (def writeTx = session.transaction(Grakn.Transaction.Type.WRITE)) {
                List<ConceptMap> getProjectAnswer = writeTx.query().match(match(
                        var("p").isa("project")
                                .has("project_name", projectName)
                ).get("p")).collect()

                if (getProjectAnswer.isEmpty()) {
                    List<ConceptMap> createProjectAnswer = writeTx.query().insert(match(var("u").iid(userId))
                            .insert(
                                    var("p").isa("project")
                                            .has("project_name", projectName)
                                            .has("create_date", LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS))
                                            .has("reference_count", 0),
                                    var().rel("has_project", var("u"))
                                            .rel("is_project", var("p")).isa("owns_project")
                            )).collect()
                    writeTx.commit()

                    def projectId = createProjectAnswer.get(0).get("p").asEntity().IID
                    log.info ("Created project: {} - Id: {}", projectName ,projectId)
                    handler.handle(Future.succeededFuture(projectId))
                } else {
                    handler.handle(Future.succeededFuture(getProjectAnswer.get(0).get("p").asEntity().IID))
                }
            }
        }, false, handler)
    }

    void getProjectId(String projectName, Handler<AsyncResult<String>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                List<ConceptMap> projectIdAnswer = readTx.query().match(match(
                        var("p").isa("project")
                                .has("name", projectName)
                ).get("p")).collect()

                if (projectIdAnswer.isEmpty()) {
                    handler.handle(Future.succeededFuture())
                } else {
                    handler.handle(Future.succeededFuture(projectIdAnswer.get(0).get("p").asEntity().IID))
                }
            }
        }, false, handler)
    }

    void getFileCount(String projectName, Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                def fileCountAnswer = readTx.query().match(match(
                        var("p").isa("project")
                                .has("name", projectName),
                        var("fi").isa("file"),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file")
                ).get("fi").count()).get()

                handler.handle(Future.succeededFuture(fileCountAnswer.asLong()))
            }
        }, false, handler)
    }

    void getFunctionCount(String projectName, Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                def functionCountAnswer = readTx.query().match(match(
                        var("p").isa("project")
                                .has("name", projectName),
                        var("fi").isa("file"),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file"),
                        var("f").isa("function"),
                        var().rel("has_defines_function", var("fi"))
                                .rel("is_defines_function", var("f")).isa("defines_function")
                ).get("f").count()).get()

                handler.handle(Future.succeededFuture(functionCountAnswer.asLong()))
            }
        }, false, handler)
    }

    void getFunctionIds(String projectIdOrName, Handler<AsyncResult<List<String>>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                def projectIdOrNameVar
                if (projectIdOrName.contains("/")) {
                    projectIdOrNameVar = var("p").has("project_name", projectIdOrName)
                } else {
                    projectIdOrNameVar = var("p").iid(projectIdOrName)
                }
                List<ConceptMap> functionIdsAnswer = readTx.query().match(match(
                        projectIdOrNameVar,
                        var("fi").isa("file"),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file"),
                        var("f").isa("function"),
                        var().rel("has_defines_function", var("fi"))
                                .rel("is_defines_function", var("f")).isa("defines_function")
                ).get("f")).collect()

                handler.handle(Future.succeededFuture(
                        functionIdsAnswer.stream().map({ it.get("f").asEntity().IID })
                                .collect(Collectors.toList())
                ))
            }
        }, false, handler)
    }

    void getMostReferencedFunctionsInformation(String projectName, int limit,
                                               Handler<AsyncResult<List<FunctionReferenceInformation>>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                List<ConceptMap> mostReferencedFunctionsAnswer = readTx.query().match(match(
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
                ).get("f", "k_uri", "q_name", "ref_count").sort("ref_count", "desc")
                        .limit(limit)).collect()

                def result = []
                mostReferencedFunctionsAnswer.each {
                    def functionId = it.get("f").asEntity().IID
                    def kytheUri = it.get("k_uri").asAttribute().asString().getValue()
                    def qualifiedName = it.get("q_name").asAttribute().asString().getValue()
                    def referenceCount = it.get("ref_count").asAttribute().asLong().getValue() as int
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
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                def callerFunctionOrs = new ArrayList<ThingVariable>()
                def callerProjectOrs = new ArrayList<ThingVariable>()
                partialFunctionReferences.each {
                    callerFunctionOrs << var("f").iid(it.functionId)
                    callerProjectOrs << var("p_ref").iid(it.projectId)
                }

                List<ConceptMap> functionReferencesAnswer = readTx.query().match(match(
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
                ).get("f", "k_uri", "q_name", "f_loc", "p_name").limit(limit)).collect()

                def result = []
                functionReferencesAnswer.each {
                    def functionId = it.get("f").asEntity().IID
                    def ref = functionMap.get(functionId)
                    ref.kytheUri = it.get("k_uri").asAttribute().asString().getValue()
                    ref.qualifiedName = it.get("q_name").asAttribute().asString().getValue()
                    ref.fileLocation = it.get("f_loc").asAttribute().asString().getValue()
                    ref.projectName = it.get("p_name").asAttribute().asString().getValue()
                    result << ref
                }
                handler.handle(Future.succeededFuture(result))
            }
        }, false, handler)
    }

    void getOrCreateFile(String projectId, String qualifiedName, String fileLocation,
                         Handler<AsyncResult<String>> handler) {
        vertx.executeBlocking({
            try (def writeTx = session.transaction(Grakn.Transaction.Type.WRITE)) {
                List<ConceptMap> getFileAnswer = writeTx.query().match(match(
                        var("p").iid(projectId),
                        var("fi").isa("file")
                                .has("qualified_name", qualifiedName),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file")
                ).get("fi")).collect()

                if (getFileAnswer.isEmpty()) {
                    List<ConceptMap> createFileAnswer = writeTx.query().insert(match(var("p").iid(projectId)).insert(
                            var("fi").isa("file")
                                    .has("qualified_name", qualifiedName)
                                    .has("language", SourceLanguage.getSourceLanguage(fileLocation).key)
                                    .has("file_location", fileLocation)
                                    .has("reference_count", 0),
                            var().rel("has_defines_file", var("p"))
                                    .rel("is_defines_file", var("fi")).isa("defines_file")
                    )).collect()
                    writeTx.commit()
                    handler.handle(Future.succeededFuture(createFileAnswer.get(0).get("fi").asEntity().IID))
                } else {
                    handler.handle(Future.succeededFuture(getFileAnswer.get(0).get("fi").asEntity().IID))
                }
            }
        }, false, handler)
    }

    void getOrCreateFiles(List<Tuple3<String, String, String>> fileData,
                          Handler<AsyncResult<List<String>>> handler) {
        vertx.executeBlocking({
            try (def writeTx = session.transaction(Grakn.Transaction.Type.WRITE)) {
                def fileIds = new ArrayList<String>()
                boolean wroteData = false
                fileData.each {
                    List<ConceptMap> getFileAnswer = writeTx.query().match(match(
                            var("p").iid(it.v1),
                            var("fi").isa("file")
                                    .has("qualified_name", it.v2),
                            rel("has_defines_file", var("p"))
                                    .rel("is_defines_file", var("fi")).isa("defines_file")
                    ).get("fi")).collect()

                    if (getFileAnswer.isEmpty()) {
                        List<ConceptMap> createFileAnswer = writeTx.query().insert(match(var("p").iid(it.v1)).insert(
                                var("fi").isa("file")
                                        .has("qualified_name", it.v2)
                                        .has("language", SourceLanguage.getSourceLanguage(it.v3).key)
                                        .has("file_location", it.v3)
                                        .has("reference_count", 0),
                                var().rel("has_defines_file", var("p"))
                                        .rel("is_defines_file", var("fi")).isa("defines_file")
                        )).collect()
                        wroteData = true
                        fileIds << createFileAnswer.get(0).get("fi").asEntity().IID
                    } else {
                        fileIds << getFileAnswer.get(0).get("fi").asEntity().IID
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
            try (def writeTx = session.transaction(Grakn.Transaction.Type.WRITE)) {
                def functionId = getOrCreateFunction(writeTx, fileId, function, referenceCount)
                writeTx.commit()
                handler.handle(Future.succeededFuture(functionId))
            } catch (all) {
                handler.handle(Future.failedFuture(all))
            }
        }, false, handler)
    }

    static String getOrCreateFunction(Grakn.Transaction writeTx, String fileId,
                                      FunctionInformation function, int referenceCount) {
        List<ConceptMap> getFunctionAnswer = writeTx.query().match(match(
                var("f").isa("function")
                        .has("kythe_uri", function.kytheUri)
                        .has("qualified_name", var("q_name"))
        ).get("f", "q_name", "q_name_rel")).collect()

        if (getFunctionAnswer.isEmpty()) {
            log.info("createFunction - File id: $fileId - Function: $function - Reference count: $referenceCount")
            List<ConceptMap> createFunctionAnswer
            if (fileId != null) {
                createFunctionAnswer = writeTx.query().insert(match(var("fi").iid(fileId)).insert(
                        var("f").isa("function")
                                .has("kythe_uri", function.kytheUri)
                                .has("qualified_name", function.qualifiedName)
                                .has("reference_count", referenceCount),
                        var().rel("has_defines_function", var("fi"))
                                .rel("is_defines_function", var("f")).isa("defines_function")
                )).collect()
            } else {
                createFunctionAnswer = writeTx.query().insert(insert(
                        var("f").isa("function")
                                .has("kythe_uri", function.kytheUri)
                                .has("qualified_name", function.qualifiedName)
                                .has("reference_count", referenceCount)
                )).collect()
            }
            return createFunctionAnswer.get(0).get("f").asEntity().IID
        } else {
            log.info("getFunction - File id: $fileId - Function: $function - Reference count: $referenceCount")
            if (fileId != null) {
                //function definer; ensure qualified name is correct
                def currentQualifiedName = getFunctionAnswer.get(0).get("q_name").asAttribute().asString().getValue()
                def qualifiedNameRelId = getFunctionAnswer.get(0).get("q_name_rel").asRelation().IID
                def functionId = getFunctionAnswer.get(0).get("f").asEntity().IID
                if (currentQualifiedName != function.qualifiedName) {
                    writeTx.query().match(parseQuery(
                            'match $r id ' + qualifiedNameRelId + '; delete $r;'
                    ) as GraqlMatch)
                    writeTx.query().insert(match(
                            var("f").iid(functionId)).insert(
                            var("f").has("qualified_name", function.qualifiedName)
                    ))
                }

                //function definer; ensure function is associated to file
                def getFunctionFileRelationAnswer = writeTx.query().match(match(
                        var("f").iid(functionId),
                        var("fi").iid(fileId),
                        var().rel("has_defines_function", var("fi"))
                                .rel("is_defines_function", var("f")).isa("defines_function")
                ).get()).collect()
                if (getFunctionFileRelationAnswer.isEmpty()) {
                    writeTx.query().insert(match(
                            var("f").iid(functionId),
                            var("fi").iid(fileId)
                    ).insert(
                            var().rel("has_defines_function", var("fi"))
                                    .rel("is_defines_function", var("f")).isa("defines_function")
                    ))
                }
            }
            return getFunctionAnswer.get(0).get("f").asEntity().IID
        }
    }

    void insertFunctionReference(String projectId, String callerFileId,
                                 Instant callerCommitDate, String callerCommitSha1, int callerLineNumber,
                                 FunctionInformation callerFunction, FunctionInformation calleeFunction,
                                 Handler<AsyncResult<Void>> handler) {
        def callerFunctionFut = Promise.promise()
        getOrCreateFunction(callerFileId, callerFunction, 0, callerFunctionFut)
        def calleeFunctionFut = Promise.promise()
        getOrCreateFunction(calleeFunction, 1, calleeFunctionFut)
        CompositeFuture.all(callerFunctionFut.future(), calleeFunctionFut.future()).onComplete({
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

    void getOrCreateFunctionReference(String callerFunctionId, String calleeFunctionId,
                                      Handler<AsyncResult<String>> handler) {
        vertx.executeBlocking({
            try (def writeTx = session.transaction(Grakn.Transaction.Type.WRITE)) {
                def functionReferenceId = getOrCreateFunctionReference(writeTx, callerFunctionId, calleeFunctionId)
                writeTx.commit()
                handler.handle(Future.succeededFuture(functionReferenceId))
            } catch (all) {
                handler.handle(Future.failedFuture(all))
            }
        }, false, handler)
    }

    static String getOrCreateFunctionReference(Grakn.Transaction writeTx,
                                               String callerFunctionId, String calleeFunctionId) {
        List<ConceptMap> getReferenceAnswer = writeTx.query().match(match(
                var("f1").iid(callerFunctionId),
                var("f2").iid(calleeFunctionId),
                var("ref").rel("has_reference_call", var("f1"))
                        .rel("is_reference_call", var("f2")).isa("reference_call")
        ).get("ref")).collect()

        if (getReferenceAnswer.isEmpty()) {
            List<ConceptMap> createReferenceAnswer = writeTx.query().insert(match(
                    var("f1").iid(callerFunctionId),
                    var("f2").iid(calleeFunctionId)
            ).insert(
                    var("ref").rel("has_reference_call", var("f1"))
                            .rel("is_reference_call", var("f2")).isa("reference_call")
            )).collect()
            return createReferenceAnswer.get(0).get("ref").asRelation().IID
        } else {
            return getReferenceAnswer.get(0).get("ref").asRelation().IID
        }
    }
}
