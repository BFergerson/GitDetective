package io.gitdetective.web.task

import ai.grakn.Grakn
import ai.grakn.GraknSession
import ai.grakn.Keyspace
import io.gitdetective.web.WebServices
import io.gitdetective.web.dao.GraknDAO
import io.gitdetective.web.dao.storage.ReferenceStorage
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory

import java.util.concurrent.TimeUnit

/**
 * todo: description
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class RefreshFunctionLeaderboard extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(RefreshFunctionLeaderboard.class)
    private final ReferenceStorage referenceStorage
    private final GraknDAO grakn
    private GraknSession graknSession
    private JsonArray cachedLeaderboardResults = new JsonArray()

    RefreshFunctionLeaderboard(ReferenceStorage referenceStorage, GraknDAO grakn) {
        this.referenceStorage = referenceStorage
        this.grakn = grakn
    }

    @Override
    void start() throws Exception {
        String graknHost = config().getString("grakn.host")
        int graknPort = config().getInteger("grakn.port")
        String graknKeyspace = config().getString("grakn.keyspace")
        def keyspace = Keyspace.of(graknKeyspace)
        graknSession = Grakn.session(graknHost + ":" + graknPort, keyspace)

        updateFunctionLeaderboard()
        vertx.setPeriodic(TimeUnit.HOURS.toMillis(1), {
            updateFunctionLeaderboard()
            log.info "Refreshed function leaderboard"
        })

        vertx.eventBus().consumer(WebServices.GET_FUNCTION_LEADERBOARD, {
            it.reply(cachedLeaderboardResults.copy())
        })
    }

    private void updateFunctionLeaderboard() {
        referenceStorage.getFunctionLeaderboard({
            if (it.failed()) {
                println "do something"
            } else {
                def topFunctions = it.result()
                def futures = new ArrayList<Future>()
                for (int i = 0; i < topFunctions.size(); i++) {
                    def function = topFunctions.getJsonObject(i)
                    def functionId = function.getString("function_id")
                    def fut = Future.future()
                    futures.add(fut)
                    grakn.getFunctionQualifiedName(functionId, {
                        if (it.failed()) {
                            fut.fail(it.cause())
                        } else {
                            if (it.result() == null) {
                                fut.fail(new NullPointerException("Null qualified name! Function id: $functionId"))
                            } else {
                                function.put("qualified_name", it.result())
                                fut.complete()
                            }
                        }
                    })

                    CompositeFuture.all(futures).setHandler({
                        if (it.failed()) {
                            println "do something"
                        } else {
                            cachedLeaderboardResults = topFunctions.copy()
                        }
                    })
                }
            }
        })
    }

}
