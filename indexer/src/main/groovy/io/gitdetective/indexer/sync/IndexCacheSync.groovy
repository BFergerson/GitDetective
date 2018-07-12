package io.gitdetective.indexer.sync

import io.gitdetective.web.dao.RedisDAO
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import org.mapdb.DB
import org.mapdb.DBMaker
import org.mapdb.Serializer

class IndexCacheSync extends AbstractVerticle {

    private final RedisDAO redis
    private final DB db
    private Set<String> definitions
    private Set<String> references

    IndexCacheSync(RedisDAO redis) {
        this.redis = redis
        db = DBMaker.fileDB(new File(config().getString("working_directory"), "def-ref.cache"))
                .transactionEnable().make()
    }

    @Override
    void start(Future<Void> startFuture) throws Exception {
        definitions = db.hashSet("definitions", Serializer.STRING).createOrOpen()
        references = db.hashSet("references", Serializer.STRING).createOrOpen()

        vertx.eventBus().consumer("io.vertx.redis." + RedisDAO.NEW_DEFINITION, {
            def msg = (it.body() as JsonObject).getJsonObject("value").getString("message")
            definitions.add(msg)
        })
        vertx.eventBus().consumer("io.vertx.redis." + RedisDAO.NEW_REFERENCE, {
            def msg = (it.body() as JsonObject).getJsonObject("value").getString("message")
            references.add(msg)
        })

        def defFut = Future.future()
        def refFut = Future.future()
        redis.client.subscribe(RedisDAO.NEW_DEFINITION, defFut.completer())
        redis.client.subscribe(RedisDAO.NEW_REFERENCE, refFut.completer())
        CompositeFuture.all(defFut, refFut).setHandler(startFuture.completer())
        println "IndexCacheSync started"
    }

    @Override
    void stop() throws Exception {
        db.close()
    }

    boolean hasDefinition(String fileId, String functionId) {
        return definitions.contains(fileId + '-' + functionId)
    }

    boolean hasReference(String fileOrFunctionId, String functionId) {
        return references.contains(fileOrFunctionId + '-' + functionId)
    }

}
