package io.gitdetective.web.task

import ai.grakn.Grakn
import ai.grakn.GraknSession
import ai.grakn.Keyspace
import io.gitdetective.web.dao.GraknDAO
import io.gitdetective.web.dao.RedisDAO
import io.gitdetective.web.dao.storage.ReferenceStorage
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
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
    private final RedisDAO redis
    private final ReferenceStorage referenceStorage
    private final GraknDAO grakn
    private GraknSession graknSession

    RefreshFunctionLeaderboard(RedisDAO redis, ReferenceStorage referenceStorage, GraknDAO grakn) {
        this.redis = redis
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
    }

    private void updateFunctionLeaderboard() {
        referenceStorage.getFunctionLeaderboard(100, {
            if (it.failed()) {
                it.cause().printStackTrace()
            } else {
                def topFunctions = it.result()
                def futures = new ArrayList<Future>()
                for (int i = 0; i < topFunctions.size(); i++) {
                    def function = topFunctions.getJsonObject(i)
                    def functionId = function.getString("function_id")
                    if (function.getValue("external_reference_count") instanceof String) {
                        function.put("external_reference_count", function.getString("external_reference_count") as long)
                    }

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
                            it.cause().printStackTrace()
                        } else {
                            redis.cacheFunctionLeaderboard(topFunctions, {
                                if (it.failed()) {
                                    it.cause().printStackTrace()
                                }
                            })
                        }
                    })
                }
            }
        })
    }

}
