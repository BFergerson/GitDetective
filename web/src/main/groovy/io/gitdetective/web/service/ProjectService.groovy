package io.gitdetective.web.service

import grakn.client.GraknClient
import io.gitdetective.web.service.model.FunctionReferenceInformation
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler

import static graql.lang.Graql.match
import static graql.lang.Graql.var

class ProjectService extends AbstractVerticle {

    private final GraknClient.Session session

    ProjectService(GraknClient.Session session) {
        this.session = session
    }

    void getMostReferencedFunctionsInformation(String projectName, int limit,
                                               Handler<AsyncResult<List<FunctionReferenceInformation>>> handler) {
        vertx.executeBlocking({
            try (def readTx = session.transaction().read()) {
                def mostReferencedFunctions = readTx.execute(match(
                        var("p").isa("project")
                                .has("name", projectName),
                        var("fi").isa("file"),
                        var().rel("has_defines_file", var("p"))
                                .rel("is_defines_file", var("fi")).isa("defines_file"),
                        var("f").isa("function")
                                .has("kythe_uri", var("k_uri"))
                                .has("qualified_name", var("q_name"))
                                .has("reference_count", var("ref_count")),
                        var().rel("has_defines_function", var("fi"))
                                .rel("is_defines_function", var("f")).isa("defines_function"),
                ).get("k_uri", "q_name", "ref_count").sort("ref_count", "desc").limit(limit))

                def result = []
                mostReferencedFunctions.each {
                    def kytheUri = it.get("k_uri").asAttribute().value() as String
                    def qualifiedName = it.get("q_name").asAttribute().value() as String
                    def referenceCount = it.get("ref_count").asAttribute().value() as int
                    result << new FunctionReferenceInformation(kytheUri, qualifiedName, referenceCount)
                }
                handler.handle(Future.succeededFuture(result))
            }
        }, false, handler)
    }
}
