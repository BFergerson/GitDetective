package io.gitdetective.web.task

import grakn.client.GraknClient
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import static graql.lang.Graql.*

class UpdateProjectReferenceCounts extends AbstractVerticle {

    public static final String PERFORM_TASK_NOW = "UpdateProjectReferenceCounts"

    private final GraknClient.Session graknSession

    UpdateProjectReferenceCounts(GraknClient.Session graknSession) {
        this.graknSession = graknSession
    }

    @Override
    void start() throws Exception {
        //todo: once a day
        vertx.eventBus().consumer(PERFORM_TASK_NOW, request -> {
            updateGraknProjectReferenceCounts({
                if (it.succeeded()) {
                    request.reply(true)
                } else {
                    request.fail(500, it.cause().message)
                }
            })
        })
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
