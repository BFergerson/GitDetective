package io.gitdetective.web.work

import grakn.client.GraknClient
import groovy.util.logging.Slf4j
import io.gitdetective.web.WebLauncher
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import java.util.concurrent.TimeUnit

import static graql.lang.Graql.*

@Slf4j
class UpdateFileReferenceCounts extends AbstractVerticle {

    public static final String PERFORM_TASK_NOW = "UpdateFileReferenceCounts"

    private final GraknClient.Session graknSession

    UpdateFileReferenceCounts(GraknClient.Session graknSession) {
        this.graknSession = graknSession
    }

    @Override
    void start() throws Exception {
        def timer = WebLauncher.metrics.timer(PERFORM_TASK_NOW)
        vertx.eventBus().consumer(PERFORM_TASK_NOW, request -> {
            def time = timer.time()
            log.info(PERFORM_TASK_NOW + " started")

            updateGraknFileReferenceCounts({
                if (it.succeeded()) {
                    request.reply(true)
                } else {
                    request.fail(500, it.cause().message)
                }

                log.info(PERFORM_TASK_NOW + " finished")
                time.close()
            })
        })

        //perform every 45 minutes
        vertx.setPeriodic(TimeUnit.MINUTES.toMillis(45), {
            vertx.eventBus().send(PERFORM_TASK_NOW, true)
        })
        vertx.eventBus().send(PERFORM_TASK_NOW, true) //perform on boot
    }

    void updateGraknFileReferenceCounts(Handler<AsyncResult<Void>> handler) {
        def writeTx = graknSession.transaction().write()
        writeTx.stream(match(
                var("fi").isa("file")
                        .has("reference_count", var("fi_ref_count"), var("fi_ref_count_relation"))
        ).get()).forEach({
            def fileId = it.get("fi").asEntity().id().value
            def fileRefCount = it.get("fi_ref_count").asAttribute().value() as long
            def fileRefCountRelationId = it.get("fi_ref_count_relation").asRelation().id().value

            def actualFileRefCount = writeTx.execute(match(
                    var("fi").id(fileId),
                    var("f").isa("function").has("reference_count", var("f_ref_count")),
                    var().rel("has_defines_function", var("fi"))
                            .rel("is_defines_function", var("f")).isa("defines_function")
            ).get("f_ref_count").sum("f_ref_count")).get(0).number().longValue()
            if (actualFileRefCount != fileRefCount) {
                writeTx.execute(parse(
                        'match $r id ' + fileRefCountRelationId + '; delete $r;'
                ))
                writeTx.execute(match(
                        var("fi").id(fileId)).insert(
                        var("fi").has("reference_count", actualFileRefCount)
                ))
            }
        })
        writeTx.commit()
        handler.handle(Future.succeededFuture())
    }
}
