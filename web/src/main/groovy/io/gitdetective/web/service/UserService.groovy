package io.gitdetective.web.service

import grakn.client.GraknClient
import groovy.util.logging.Slf4j
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
                                .has("user_name", username),
                        var("p").isa("project"),
                        var().rel("has_project", var("u"))
                                .rel("is_project", var("p")).isa("owns_project")
                ).get("p").count())

                handler.handle(Future.succeededFuture(projectCountAnswer.get(0).number().longValue()))
            }
        }, false, handler)
    }

    void getMostReferencedProjectsInformation() {
    }
}
