package io.gitdetective.web.service

import grakn.client.GraknClient
import groovy.util.logging.Slf4j
import io.gitdetective.web.service.model.ProjectReferenceInformation
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import static graql.lang.Graql.match
import static graql.lang.Graql.var

@Slf4j
class UserService extends AbstractVerticle {

    private final GraknClient.Session session

    UserService(GraknClient.Session session) {
        this.session = Objects.requireNonNull(session)
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
