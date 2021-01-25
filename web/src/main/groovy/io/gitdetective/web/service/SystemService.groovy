package io.gitdetective.web.service

import grakn.client.Grakn
import grakn.client.concept.answer.ConceptMap
import grakn.client.rpc.RPCSession
import groovy.util.logging.Slf4j
import io.gitdetective.web.dao.PostgresDAO
import io.gitdetective.web.model.FunctionReferenceInformation
import io.gitdetective.web.model.ProjectReferenceInformation
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import static graql.lang.Graql.*

@Slf4j
class SystemService extends AbstractVerticle {

    private final RPCSession.Core session
    private final PostgresDAO postgres

    SystemService(RPCSession.Core session, PostgresDAO postgres) {
        this.session = Objects.requireNonNull(session)
        this.postgres = Objects.requireNonNull(postgres)
    }

    void getTotalUniqueReferenceCount(Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                handler.handle(Future.succeededFuture(0L))
//                def totalFileCountAnswer = readTx.execute(compute().count().in("reference_call"))
//                handler.handle(Future.succeededFuture(totalFileCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getTotalReferenceCount(Handler<AsyncResult<Long>> handler) {
        postgres.client.query("SELECT COUNT(*) from function_reference").execute({
            handler.handle(Future.succeededFuture(0L))
//            if (it.succeeded()) {
//                handler.handle(Future.succeededFuture(it.result().toList().get(0).getLong(0)))
//            } else {
//                handler.handle(Future.failedFuture(it.cause()))
//            }
        })
    }

    void getTotalProjectCount(Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                handler.handle(Future.succeededFuture(0L))
//                def totalProjectCountAnswer = readTx.execute(compute().count().in("project"))
//                handler.handle(Future.succeededFuture(totalProjectCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getTotalFileCount(Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                handler.handle(Future.succeededFuture(0L))
//                def totalFileCountAnswer = readTx.execute(compute().count().in("file"))
//                handler.handle(Future.succeededFuture(totalFileCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getTotalFunctionCount(Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                handler.handle(Future.succeededFuture(0L))
//                def totalFunctionCountAnswer = readTx.execute(compute().count().in("function"))
//                handler.handle(Future.succeededFuture(totalFunctionCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getTotalMostReferencedProjectsInformation(int limit,
                                                   Handler<AsyncResult<List<ProjectReferenceInformation>>> handler) {
        log.info("getTotalMostReferencedProjectsInformation - Limit: " + limit)
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                List<ConceptMap> totalMostReferencedProjectsAnswer = readTx.query().match(match(
                        var("p").isa("project")
                                .has("project_name", var("p_name"))
                                .has("reference_count", var("ref_count"))
                ).get("p_name", "ref_count").sort("ref_count", "desc").limit(limit)).collect()

                log.info("getTotalMostReferencedProjectsInformation - Found: " + totalMostReferencedProjectsAnswer.size())
                if (totalMostReferencedProjectsAnswer.isEmpty()) {
                    handler.handle(Future.succeededFuture(Collections.emptyList()))
                    return
                }

                def result = []
                totalMostReferencedProjectsAnswer.each {
                    def projectName = it.get("p_name").asAttribute().asString().value
                    def referenceCount = it.get("ref_count").asAttribute().asLong().value as int
                    result << new ProjectReferenceInformation(projectName, referenceCount)
                }
                handler.handle(Future.succeededFuture(result))
            }
        }, false, handler)
    }

    void getTotalMostReferencedFunctionsInformation(int limit,
                                                    Handler<AsyncResult<List<FunctionReferenceInformation>>> handler) {
        log.info("getTotalMostReferencedFunctionsInformation - Limit: " + limit)
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                List<ConceptMap> totalMostReferencedFunctionsAnswer = readTx.query().match(match(
                        var("f").isa("function")
                                .has("kythe_uri", var("k_uri"))
                                .has("qualified_name", var("q_name"))
                                .has("reference_count", var("ref_count"))
                ).get("f", "k_uri", "q_name", "ref_count").sort("ref_count", "desc").limit(limit)).collect()

                log.info("getTotalMostReferencedFunctionsInformation - Found: " + totalMostReferencedFunctionsAnswer.size())
                if (totalMostReferencedFunctionsAnswer.isEmpty()) {
                    handler.handle(Future.succeededFuture(Collections.emptyList()))
                    return
                }

                def functionIdOrs = []
                totalMostReferencedFunctionsAnswer.each {
                    functionIdOrs << var("f").iid(it.get("f").asEntity().IID)
                }
                def getFileProjectsAnswer = readTx.query().match(match(
                        or(functionIdOrs),
                        var("fi").isa("file"),
                        var().rel("has_defines_function", var("fi"))
                                .rel("is_defines_function", var("f")).isa("defines_function"),
                        var("p").isa("project")
                                .has("name", var("p_name")),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file")
                ).get("f", "p_name"))
                def projectNameMap = new HashMap<String, String>()
                getFileProjectsAnswer.each {
                    def functionId = it.get("f").asEntity().IID
                    def projectName = it.get("p_name").asAttribute().asString().getValue()
                    projectNameMap.put(functionId, projectName)
                }

                def result = []
                totalMostReferencedFunctionsAnswer.each {
                    def functionId = it.get("f").asEntity().IID
                    def kytheUri = it.get("k_uri").asAttribute().asString().getValue()
                    def qualifiedName = it.get("q_name").asAttribute().asString().getValue()
                    def referenceCount = it.get("ref_count").asAttribute().asLong().getValue() as int
                    def projectName = projectNameMap.get(functionId)
                    result << new FunctionReferenceInformation(functionId, kytheUri, qualifiedName, referenceCount, projectName)
                }
                handler.handle(Future.succeededFuture(result))
            }
        }, false, handler)
    }
}
