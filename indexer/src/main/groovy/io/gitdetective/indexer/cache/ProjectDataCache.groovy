package io.gitdetective.indexer.cache

import io.gitdetective.web.dao.RedisDAO
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import org.mapdb.DB
import org.mapdb.DBMaker
import org.mapdb.Serializer

/**
 * todo: description
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class ProjectDataCache extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(ProjectDataCache.class)
    private final RedisDAO redis
    private DB db
    private Set<String> definitions
    private Set<String> references
    private Map<String, String> files
    private Map<String, String> functions

    ProjectDataCache(RedisDAO redis) {
        this.redis = redis
    }

    @Override
    void start(Future<Void> startFuture) throws Exception {
        //init project data cache
        db = DBMaker.fileDB(new File(config().getString("working_directory"), "gd-project.cache"))
                .closeOnJvmShutdown()
                .transactionEnable().make()
        definitions = db.hashSet("definitions", Serializer.STRING).createOrOpen()
        references = db.hashSet("references", Serializer.STRING).createOrOpen()
        files = db.hashMap("files", Serializer.STRING, Serializer.STRING).createOrOpen()
        functions = db.hashMap("functions", Serializer.STRING, Serializer.STRING).createOrOpen()
        vertx.setPeriodic(config().getInteger("project_data_flush_ms"), {
            db.commit()
            log.debug("Flushed project index cache")
            log.debug("Cached definitions: " + definitions.size())
            log.debug("Cached references: " + references.size())
            log.debug("Cached files: " + files.size())
            log.debug("Cached functions: " + functions.size())
        })

        //setup message consumers
        vertx.eventBus().consumer("io.vertx.redis." + RedisDAO.NEW_PROJECT_FILE, {
            def msg = (it.body() as JsonObject).getJsonObject("value").getString("message")
            def msgParts = msg.split("\\|")
            def project = msgParts[0]
            def filename = msgParts[1]
            def fileId = msgParts[2]
            files.put(project + ":" + filename, fileId)
        })
        vertx.eventBus().consumer("io.vertx.redis." + RedisDAO.NEW_PROJECT_FUNCTION, {
            def msg = (it.body() as JsonObject).getJsonObject("value").getString("message")
            def msgParts = msg.split("\\|")
            def project = msgParts[0]
            def functionName = msgParts[1]
            def functionId = msgParts[2]
            functions.put(project + ":" + functionName, functionId)
        })
        vertx.eventBus().consumer("io.vertx.redis." + RedisDAO.NEW_DEFINITION, {
            def msg = (it.body() as JsonObject).getJsonObject("value").getString("message")
            definitions.add(msg)
        })
        vertx.eventBus().consumer("io.vertx.redis." + RedisDAO.NEW_REFERENCE, {
            def msg = (it.body() as JsonObject).getJsonObject("value").getString("message")
            references.add(msg)
        })

        def fileFut = Future.future()
        def funcFut = Future.future()
        def defFut = Future.future()
        def refFut = Future.future()
        redis.client.subscribe(RedisDAO.NEW_PROJECT_FILE, fileFut.completer())
        redis.client.subscribe(RedisDAO.NEW_PROJECT_FUNCTION, funcFut.completer())
        redis.client.subscribe(RedisDAO.NEW_DEFINITION, defFut.completer())
        redis.client.subscribe(RedisDAO.NEW_REFERENCE, refFut.completer())
        CompositeFuture.all(fileFut, funcFut, defFut, refFut).setHandler(startFuture.completer())
        log.info "ProjectDataCache started"
    }

    @Override
    void stop() throws Exception {
        db?.close()
    }

    Optional<String> getProjectFileId(String project, String fileName) {
        def result = files.get(project + ":" + fileName)
        if (result == null) {
            return Optional.empty()
        }
        return Optional.of(result)
    }

    Optional<String> getProjectFunctionId(String project, String functionName) {
        def result = functions.get(project + ":" + functionName)
        if (result == null) {
            return Optional.empty()
        }
        return Optional.of(result)
    }

    boolean hasDefinition(String fileId, String functionId) {
        return definitions.contains(fileId + '-' + functionId)
    }

    boolean hasReference(String fileOrFunctionId, String functionId) {
        return references.contains(fileOrFunctionId + '-' + functionId)
    }

}
