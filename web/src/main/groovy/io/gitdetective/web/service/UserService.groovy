package io.gitdetective.web.service

import grakn.client.GraknClient
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

    private final GraknClient.Session session

    UserService(GraknClient.Session session) {
        this.session = Objects.requireNonNull(session)
    }

    void getOrCreateUser(String username, Handler<AsyncResult<String>> handler) {
        vertx.executeBlocking({
            try (def writeTx = session.transaction().write()) {
                def getUserAnswer = writeTx.execute(match(
                        var("u").isa("user")
                                .has("username", username)
                ).get("u"))

                if (getUserAnswer.isEmpty()) {
                    def createUserAnswer = writeTx.execute(insert(
                            var("u").isa("user")
                                    .has("username", username)
                                    .has("create_date", LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS))
                    ))
                    writeTx.commit()
                    handler.handle(Future.succeededFuture(createUserAnswer.get(0).get("u").asEntity().id().value))
                } else {
                    handler.handle(Future.succeededFuture(getUserAnswer.get(0).get("u").asEntity().id().value))
                }
            }
        }, false, handler)
    }

    void getProjectCount(String username, Handler<AsyncResult<Long>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def projectCountAnswer = readTx.execute(match(
                        var("u").isa("user")
                                .has("username", username),
                        var("p").isa("project"),
                        var().rel("has_project", var("u"))
                                .rel("is_project", var("p")).isa("owns_project")
                ).get("p").count())

                handler.handle(Future.succeededFuture(projectCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getMostReferencedProjectsInformation(String username, int limit,
                                              Handler<AsyncResult<List<ProjectReferenceInformation>>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def mostReferencedProjectsAnswer = readTx.execute(match(
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
                    def projectName = it.get("p_name").asAttribute().value() as String
                    def referenceCount = it.get("ref_count").asAttribute().value() as int
                    result << new ProjectReferenceInformation(projectName, referenceCount)
                }
                handler.handle(Future.succeededFuture(result))
            }
        }, false, handler)
    }
}
