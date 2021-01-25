package io.gitdetective.web.work

import grakn.client.Grakn
import grakn.client.rpc.RPCSession
import groovy.util.logging.Slf4j
import io.gitdetective.web.WebLauncher
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import java.util.concurrent.TimeUnit

import static graql.lang.Graql.match
import static graql.lang.Graql.parseQuery
import static graql.lang.Graql.var

@Slf4j
class UpdateProjectReferenceCounts extends AbstractVerticle {

    public static final String PERFORM_TASK_NOW = "UpdateProjectReferenceCounts"

    private final RPCSession.Core graknSession

    UpdateProjectReferenceCounts(RPCSession.Core graknSession) {
        this.graknSession = graknSession
    }

    @Override
    void start() throws Exception {
        def timer = WebLauncher.metrics.timer(PERFORM_TASK_NOW)
        vertx.eventBus().consumer(PERFORM_TASK_NOW, request -> {
            def time = timer.time()
            log.info(PERFORM_TASK_NOW + " started")

            updateGraknProjectReferenceCounts({
                if (it.succeeded()) {
                    request.reply(true)
                } else {
                    request.fail(500, it.cause().message)
                }

                log.info(PERFORM_TASK_NOW + " finished")
                time.close()
            })
        })

        //perform every 60 minutes
        vertx.setPeriodic(TimeUnit.MINUTES.toMillis(60), {
            vertx.eventBus().send(PERFORM_TASK_NOW, true)
        })
        vertx.eventBus().send(PERFORM_TASK_NOW, true) //perform on boot
    }

    void updateGraknProjectReferenceCounts(Handler<AsyncResult<Void>> handler) {
        def writeTx = graknSession.transaction(Grakn.Transaction.Type.WRITE)
        writeTx.query().match(match(
                var("p").isa("project").has("reference_count", var("p_ref_count"))
        ).get("p", "p_ref_count")).forEach({
            def projectId = it.get("p").asEntity().IID
            def projectRefCount = it.get("p_ref_count").asAttribute().asLong().getValue()

            def actualProjectRefCount = writeTx.query().match(match(
                    var("p").iid(projectId),
                    var("fi").isa("file").has("reference_count", var("fi_ref_count")),
                    var().rel("has_defines_file", var("p"))
                            .rel("is_defines_file", var("fi")).isa("defines_file")
            ).sum("fi_ref_count")).get()
            if (!actualProjectRefCount.isLong()) {
                actualProjectRefCount = 0
            } else {
                actualProjectRefCount = actualProjectRefCount.asLong()
            }

            if (actualProjectRefCount != projectRefCount) {
                def projectRefCountRelationId = it.get("p_ref_count_relation").asRelation().IID
                writeTx.query().delete(parseQuery(
                        'match $r id ' + projectRefCountRelationId + '; delete $r;'
                ))
                writeTx.query().insert(match(
                        var("p").iid(projectId)
                ).insert(
                        var("p").has("reference_count", actualProjectRefCount)
                ))
            }
        })
        writeTx.commit()
        handler.handle(Future.succeededFuture())
    }
}
