package io.gitdetective.web.service

import grakn.client.GraknClient
import groovy.util.logging.Slf4j
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import static graql.lang.Graql.compute

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
}
