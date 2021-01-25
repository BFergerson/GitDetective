package io.gitdetective.web.service

import grakn.client.Grakn
import grakn.client.concept.answer.ConceptMap
import grakn.client.rpc.RPCSession
import groovy.util.logging.Slf4j
import io.gitdetective.web.model.ProjectReferenceInformation
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import static graql.lang.Graql.*

@Slf4j
class UserService extends AbstractVerticle {

    private final RPCSession.Core session

    UserService(RPCSession.Core session) {
        this.session = Objects.requireNonNull(session)
    }

    void getOrCreateUser(String username, Handler<AsyncResult<String>> handler) {
        vertx.executeBlocking({
            try (def writeTx = session.transaction(Grakn.Transaction.Type.WRITE)) {
                List<ConceptMap> getUserAnswer = writeTx.query().match(match(
                        var("u").isa("user")
                                .has("username", username)
                ).get("u")).collect()

                if (getUserAnswer.isEmpty()) {
                    List<ConceptMap> createUserAnswer = writeTx.query().insert(insert(
                            var("u").isa("user")
                                    .has("username", username)
                                    .has("create_date", LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS))
                    )).collect()
                    writeTx.commit()

                    //todo: grakn isn't recognizing inserted data in other transaction
                    def userId = createUserAnswer[0].get("u").asEntity().IID
                    log.info("Created user {} - Id {}", username, userId)
                    handler.handle(Future.succeededFuture(userId))
                } else {
                    handler.handle(Future.succeededFuture(getUserAnswer[0].get("u").asEntity().IID))
                }
            }
        }, false, handler)
    }

    void getProjectCount(String username, Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                def projectCountAnswer = readTx.query().match(match(
                        var("u").isa("user")
                                .has("username", username),
                        var("p").isa("project"),
                        var().rel("has_project", var("u"))
                                .rel("is_project", var("p")).isa("owns_project")
                ).get("p").count()).get()

                handler.handle(Future.succeededFuture(projectCountAnswer.asLong()))
            }
        }, false, handler)
    }

    void getMostReferencedProjectsInformation(String username, int limit,
                                              Handler<AsyncResult<List<ProjectReferenceInformation>>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction(Grakn.Transaction.Type.READ)) {
                def mostReferencedProjectsAnswer = readTx.query().match(match(
                        var("u").isa("user")
                                .has("username", username),
                        var("p").isa("project")
                                .has("project_name", var("p_name"))
                                .has("reference_count", var("ref_count")),
                        var().rel("has_project", var("u"))
                                .rel("is_project", var("p")).isa("owns_project"),
                ).get("p_name", "ref_count").sort("ref_count", "desc").limit(limit))

                def result = []
                mostReferencedProjectsAnswer.each {
                    def projectName = it.get("p_name").asAttribute().asString().getValue()
                    def referenceCount = it.get("ref_count").asAttribute().asLong().getValue() as int
                    result << new ProjectReferenceInformation(projectName, referenceCount)
                }
                handler.handle(Future.succeededFuture(result))
            }
        }, false, handler)
    }
}
