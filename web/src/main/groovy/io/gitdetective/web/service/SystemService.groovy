package io.gitdetective.web.service

import grakn.client.GraknClient
import groovy.util.logging.Slf4j
import io.gitdetective.web.model.FunctionReferenceInformation
import io.gitdetective.web.model.ProjectReferenceInformation
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import static graql.lang.Graql.*

@Slf4j
class SystemService extends AbstractVerticle {

    private final GraknClient.Session session

    SystemService(GraknClient.Session session) {
        this.session = Objects.requireNonNull(session)
    }

    void getTotalProjectCount(Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def totalProjectCountAnswer = readTx.execute(compute().count().in("project"))
                handler.handle(Future.succeededFuture(totalProjectCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getTotalFileCount(Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def totalFileCountAnswer = readTx.execute(compute().count().in("file"))
                handler.handle(Future.succeededFuture(totalFileCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getTotalFunctionCount(Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def totalFunctionCountAnswer = readTx.execute(compute().count().in("function"))
                handler.handle(Future.succeededFuture(totalFunctionCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getTotalMostReferencedProjectsInformation(int limit,
                                                   Handler<AsyncResult<List<ProjectReferenceInformation>>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def totalMostReferencedProjectsAnswer = readTx.execute(match(
                        var("p").isa("project")
                                .has("project_name", var("p_name"))
                                .has("reference_count", var("ref_count"))
                ).get("p_name", "ref_count").sort("ref_count", "desc").limit(limit))

                def result = []
                totalMostReferencedProjectsAnswer.each {
                    def projectName = it.get("p_name").asAttribute().value() as String
                    def referenceCount = it.get("ref_count").asAttribute().value() as int
                    result << new ProjectReferenceInformation(projectName, referenceCount)
                }
                handler.handle(Future.succeededFuture(result))
            }
        }, false, handler)
    }

    void getTotalMostReferencedFunctionsInformation(int limit,
                                                    Handler<AsyncResult<List<FunctionReferenceInformation>>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def totalMostReferencedFunctionsAnswer = readTx.execute(match(
                        var("f").isa("function")
                                .has("kythe_uri", var("k_uri"))
                                .has("qualified_name", var("q_name"))
                                .has("reference_count", var("ref_count"))
                ).get("f", "k_uri", "q_name", "ref_count").sort("ref_count", "desc").limit(limit))

                def result = []
                totalMostReferencedFunctionsAnswer.each {
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
