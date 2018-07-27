package io.gitdetective.web.dao

import com.google.common.base.Charsets
import com.google.common.io.Resources
import io.gitdetective.web.dao.storage.ReferenceStorage
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.asyncsql.AsyncSQLClient
import io.vertx.ext.asyncsql.PostgreSQLClient

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class PostgresDAO implements ReferenceStorage {

    public final static String ADD_FUNCTION_OWNER = Resources.toString(Resources.getResource(
            "queries/sql/storage/add_function_owner.sql"), Charsets.UTF_8)
    public final static String GET_FUNCTION_OWNERS = Resources.toString(Resources.getResource(
            "queries/sql/storage/get_function_owners.sql"), Charsets.UTF_8)
    public final static String GET_OWNED_FUNCTIONS = Resources.toString(Resources.getResource(
            "queries/sql/storage/get_owned_functions.sql"), Charsets.UTF_8)
    public final static String ADD_FUNCTION_REFERENCE = Resources.toString(Resources.getResource(
            "queries/sql/storage/add_function_reference.sql"), Charsets.UTF_8)
    public final static String ADD_IMPORTED_FILE = Resources.toString(Resources.getResource(
            "queries/sql/storage/add_imported_file.sql"), Charsets.UTF_8)
    public final static String ADD_IMPORTED_FUNCTION = Resources.toString(Resources.getResource(
            "queries/sql/storage/add_imported_function.sql"), Charsets.UTF_8)
    public final static String ADD_IMPORTED_DEFINITION = Resources.toString(Resources.getResource(
            "queries/sql/storage/add_imported_definition.sql"), Charsets.UTF_8)
    public final static String ADD_IMPORTED_REFERENCE = Resources.toString(Resources.getResource(
            "queries/sql/storage/add_imported_reference.sql"), Charsets.UTF_8)
    private final static Logger log = LoggerFactory.getLogger(PostgresDAO.class)
    private AsyncSQLClient client

    PostgresDAO(Vertx vertx, JsonObject config) {
        this.client = PostgreSQLClient.createShared(vertx, config)

        //verify postgres connection
        client.getConnection({ conn ->
            if (conn.failed()) {
                conn.cause().printStackTrace()
                System.exit(-1)
            } else {
                conn.result().query("SELECT 1", {
                    if (it.failed()) {
                        it.cause().printStackTrace()
                        System.exit(-1)
                    }
                    conn.result().close()
                })
            }
        })
    }

    @Override
    void getProjectMostExternalReferencedMethods(String githubRepository, int topCount, Handler<AsyncResult<JsonArray>> handler) {
        println "todo"
    }

    @Override
    void getMethodExternalReferences(String functionId, int offset, int limit, Handler<AsyncResult<JsonArray>> handler) {
        println "todo"
    }

    @Override
    void getFunctionTotalExternalReferenceCount(String functionId, Handler<AsyncResult<Long>> handler) {
        println "todo"
    }

    @Override
    void getProjectReferenceLeaderboard(int topCount, Handler<AsyncResult<JsonArray>> handler) {
        println "todo"
    }

    @Override
    void cacheProjectImportedFile(String githubRepository, String filename, String fileId, Handler<AsyncResult> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(githubRepository)
                params.add(filename)
                params.add(fileId)

                def conn = it.result()
                conn.queryWithParams(ADD_IMPORTED_FILE, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        handler.handle(Future.succeededFuture())
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void cacheProjectImportedFunction(String githubRepository, String functionName, String functionId, Handler<AsyncResult> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(githubRepository)
                params.add(functionName)
                params.add(functionId)

                def conn = it.result()
                conn.queryWithParams(ADD_IMPORTED_FUNCTION, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        handler.handle(Future.succeededFuture())
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void cacheProjectImportedDefinition(String fileId, String functionId, Handler<AsyncResult> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(fileId)
                params.add(functionId)

                def conn = it.result()
                conn.queryWithParams(ADD_IMPORTED_DEFINITION, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        handler.handle(Future.succeededFuture())
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void cacheProjectImportedReference(String fileOrFunctionId, String functionId, Handler<AsyncResult> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(fileOrFunctionId)
                params.add(functionId)

                def conn = it.result()
                conn.queryWithParams(ADD_IMPORTED_REFERENCE, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        handler.handle(Future.succeededFuture())
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void getOwnedFunctions(String githubRepository, Handler<AsyncResult<JsonArray>> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(githubRepository)

                def conn = it.result()
                conn.queryWithParams(GET_OWNED_FUNCTIONS, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        def rs = it.result()
                        if (!rs.rows.isEmpty()) {
                            def jsonArray = new JsonArray()
                            rs.rows.each {
                                println it
                                jsonArray.add(it)
                            }
                            handler.handle(Future.succeededFuture(jsonArray))
                        } else {
                            handler.handle(Future.succeededFuture(new JsonArray()))
                        }
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void addFunctionOwner(String functionId, String qualifiedName, String githubRepository, Handler<AsyncResult> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(githubRepository)
                params.add(functionId)
                params.add(qualifiedName)

                def conn = it.result()
                conn.queryWithParams(ADD_FUNCTION_OWNER, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        handler.handle(Future.succeededFuture())
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void getFunctionOwners(String functionId, Handler<AsyncResult<JsonArray>> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(functionId)

                def conn = it.result()
                it.result().queryWithParams(GET_FUNCTION_OWNERS, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        def rs = it.result()
                        if (!rs.rows.isEmpty()) {
                            def jsonArray = new JsonArray()
                            rs.rows.each {
                                println it
                                jsonArray.add(it)
                            }
                            handler.handle(Future.succeededFuture(jsonArray))
                        } else {
                            handler.handle(Future.succeededFuture(new JsonArray()))
                        }
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void addFunctionReference(String functionId, JsonObject fileOrFunctionReference, Handler<AsyncResult> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(functionId)
                params.add(fileOrFunctionReference)
                it.result().queryWithParams(ADD_FUNCTION_REFERENCE, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        handler.handle(Future.succeededFuture())
                    }
                })
            }
        })
    }

}
