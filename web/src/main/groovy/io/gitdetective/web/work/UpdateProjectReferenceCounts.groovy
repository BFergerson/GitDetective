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
class UpdateProjectReferenceCounts extends AbstractVerticle {

    public static final String PERFORM_TASK_NOW = "UpdateProjectReferenceCounts"

    private final GraknClient.Session graknSession

    UpdateProjectReferenceCounts(GraknClient.Session graknSession) {
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
        def writeTx = graknSession.transaction().write()
        writeTx.stream(match(
                var("p").isa("project")
                        .has("reference_count", var("p_ref_count"), var("p_ref_count_relation"))
        ).get()).forEach({
            def projectId = it.get("p").asEntity().id().value
            def projectRefCount = it.get("p_ref_count").asAttribute().value() as long
            def projectRefCountRelationId = it.get("p_ref_count_relation").asRelation().id().value

            def actualProjectRefCount = writeTx.execute(match(
                    var("p").id(projectId),
                    var("fi").isa("file").has("reference_count", var("fi_ref_count")),
                    var().rel("has_defines_file", var("p"))
                            .rel("is_defines_file", var("fi")).isa("defines_file")
            ).get("fi_ref_count").sum("fi_ref_count")).get(0).number().longValue()
            if (actualProjectRefCount != projectRefCount) {
                writeTx.execute(parse(
                        'match $r id ' + projectRefCountRelationId + '; delete $r;'
                ))
                writeTx.execute(match(
                        var("p").id(projectId)).insert(
                        var("p").has("reference_count", actualProjectRefCount)
                ))
            }
        })
        writeTx.commit()
        handler.handle(Future.succeededFuture())
    }
}
