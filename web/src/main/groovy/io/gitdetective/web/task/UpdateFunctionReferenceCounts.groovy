package io.gitdetective.web.task

import grakn.client.GraknClient
import io.gitdetective.web.dao.PostgresDAO
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.sqlclient.PreparedQuery
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.RowStream
import io.vertx.sqlclient.Transaction
import io.vertx.sqlclient.impl.ArrayTuple

import java.util.concurrent.atomic.AtomicReference

import static graql.lang.Graql.*

class UpdateFunctionReferenceCounts extends AbstractVerticle {

    public static final String PERFORM_TASK_NOW = "UpdateFunctionReferenceCounts"
    private static final String DROP_FUNCTION_REFERENCE_COUNT_TABLE =
            'DROP TABLE IF EXISTS function_reference_count'
    private static final String CREATE_FUNCTION_REFERENCE_COUNT_TABLE =
            'CREATE TABLE function_reference_count AS\n' +
                    'SELECT callee_function_id, SUM(case when deletion = false then 1 else -1 end)\n' +
                    'FROM function_reference\n' +
                    'GROUP BY callee_function_id'

    private final PostgresDAO postgres
    private final GraknClient.Session graknSession

    UpdateFunctionReferenceCounts(PostgresDAO postgres, GraknClient.Session graknSession) {
        this.postgres = postgres
        this.graknSession = graknSession
    }

    @Override
    void start() throws Exception {
        //todo: once a day
        vertx.eventBus().consumer(PERFORM_TASK_NOW, request -> {
            createFunctionReferenceCountTable({
                if (it.succeeded()) {
                    request.reply(true)
                } else {
                    request.fail(500, it.cause().message)
                }
            })
        })
    }

    void createFunctionReferenceCountTable(Handler<AsyncResult<Void>> handler) {
        postgres.client.query(DROP_FUNCTION_REFERENCE_COUNT_TABLE, {
            if (it.succeeded()) {
                postgres.client.query(CREATE_FUNCTION_REFERENCE_COUNT_TABLE, {
                    if (it.succeeded()) {
                        updateGraknReferenceCounts(handler)
                    } else {
                        handler.handle(Future.failedFuture(it.cause()))
                    }
                })
            } else {
                handler.handle(Future.failedFuture(it.cause()))
            }
        })
    }

    void updateGraknReferenceCounts(Handler<AsyncResult<Void>> handler) {
        postgres.client.getConnection({
            if (it.succeeded()) {
                def connection = it.result()
                connection.prepare("SELECT * FROM function_reference_count", {
                    if (it.succeeded()) {
                        PreparedQuery pq = it.result()
                        Transaction tx = connection.begin()
                        AtomicReference<GraknClient.Transaction> graknWriteTx
                                = new AtomicReference<>(graknSession.transaction().write())

                        RowStream<Row> stream = pq.createStream(50, ArrayTuple.EMPTY)
                        stream.exceptionHandler({
                            handler.handle(Future.failedFuture(it))
                        })
                        stream.endHandler(v -> {
                            tx.commit()
                            graknWriteTx.get().commit()
                            graknWriteTx.set(null)
                            handler.handle(Future.succeededFuture())
                        })

                        int insertCount = 0
                        stream.handler(row -> {
                            if (insertCount > 0 && insertCount % 500 == 0) {
                                graknWriteTx.get().commit()
                                graknWriteTx.set(graknSession.transaction().write())
                            }
                            insertCount++

                            graknWriteTx.get().execute(parse(
                                    'match $x id ' + row.getString(0) + ', has reference_count $ref_count via $r; delete $r;'
                            ))
                            graknWriteTx.get().execute(match(
                                    var("f").id(row.getString(0))).insert(
                                    var("f").has("reference_count", row.getLong(1))
                            ))
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
}
