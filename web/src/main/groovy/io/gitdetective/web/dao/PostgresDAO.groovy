package io.gitdetective.web.dao

import com.google.common.base.Charsets
import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.google.common.io.Resources
import io.gitdetective.web.WebServices
import io.gitdetective.web.dao.storage.ReferenceStorage
import io.vertx.core.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.asyncsql.AsyncSQLClient
import io.vertx.ext.asyncsql.PostgreSQLClient

import java.util.concurrent.TimeUnit

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class PostgresDAO implements ReferenceStorage {

    public final static String ADD_FUNCTION_OWNER = Resources.toString(Resources.getResource(
            "queries/sql/storage/add_function_owner.sql"), Charsets.UTF_8)
    public final static String REMOVE_FUNCTION_OWNER = Resources.toString(Resources.getResource(
            "queries/sql/storage/remove_function_owner.sql"), Charsets.UTF_8)
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
    public final static String GET_FUNCTION_TOTAL_EXTERNAL_REFERENCE_COUNT = Resources.toString(Resources.getResource(
            "queries/sql/storage/get_function_total_external_reference_count.sql"), Charsets.UTF_8)
    public final static String GET_FUNCTION_EXTERNAL_REFERENCES = Resources.toString(Resources.getResource(
            "queries/sql/storage/get_function_external_references.sql"), Charsets.UTF_8)
    public final static String GET_PROJECT_IMPORTED_FILE_ID = Resources.toString(Resources.getResource(
            "queries/sql/storage/get_project_imported_file_id.sql"), Charsets.UTF_8)
    public final static String GET_PROJECT_IMPORTED_FUNCTION_ID = Resources.toString(Resources.getResource(
            "queries/sql/storage/get_project_imported_function_id.sql"), Charsets.UTF_8)
    public final static String PROJECT_HAS_DEFINITION = Resources.toString(Resources.getResource(
            "queries/sql/storage/project_has_definition.sql"), Charsets.UTF_8)
    public final static String PROJECT_HAS_REFERENCE = Resources.toString(Resources.getResource(
            "queries/sql/storage/project_has_reference.sql"), Charsets.UTF_8)
    public final static String GET_FUNCTION_LEADERBOARD = Resources.toString(Resources.getResource(
            "queries/sql/storage/get_function_leaderboard.sql"), Charsets.UTF_8)
    public final static String BATCH_IMPORT_PROJECT_FILES = Resources.toString(Resources.getResource(
            "queries/sql/storage/batch_import_project_files.sql"), Charsets.UTF_8)
    public final static String BATCH_IMPORT_PROJECT_DEFINITIONS = Resources.toString(Resources.getResource(
            "queries/sql/storage/batch_import_project_definitions.sql"), Charsets.UTF_8)
    public final static String BATCH_IMPORT_PROJECT_REFERENCES = Resources.toString(Resources.getResource(
            "queries/sql/storage/batch_import_project_references.sql"), Charsets.UTF_8)
    private final static Logger log = LoggerFactory.getLogger(PostgresDAO.class)
    private final AsyncSQLClient client
    private final RedisDAO redis
    private final boolean batchSupported
    private final Cache<String, JsonArray> projectRankedFunctionsCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.MINUTES).build()

    PostgresDAO(Vertx vertx, JsonObject config, RedisDAO redis) {
        this.client = PostgreSQLClient.createShared(vertx, config)
        this.redis = redis
        this.batchSupported = config.getJsonObject("storage").getBoolean("batch_supported", false)

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
    void getProjectMostExternalReferencedFunctions(String githubRepository, int topCount, Handler<AsyncResult<JsonArray>> handler) {
        def cachedRankedFunctions = projectRankedFunctionsCache.getIfPresent(githubRepository)
        if (cachedRankedFunctions != null) {
            for (int i = 0; i < cachedRankedFunctions.size(); i++) {
                def function = cachedRankedFunctions.getJsonObject(i)
                if (!(function.getValue("external_reference_count") instanceof Long)) {
                    function.put("external_reference_count", function.getString("external_reference_count") as long)
                }
            }
            handler.handle(Future.succeededFuture(cachedRankedFunctions.copy()))
            return
        }
        getOwnedFunctions(githubRepository, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def ownedFunctions = it.result()
                def rankedOwnedFunctions = new JsonArray()
                def futures = new ArrayList<Future>()
                for (int i = 0; i < ownedFunctions.size(); i++) {
                    def function = ownedFunctions.getJsonObject(i)
                    def fut = Future.future()
                    futures.add(fut)
                    getFunctionTotalExternalReferenceCount(function.getString("function_id"), {
                        if (it.failed()) {
                            fut.fail(it.cause())
                        } else {
                            rankedOwnedFunctions.add(function.put("external_reference_count", it.result()))
                            fut.complete()
                        }
                    })
                }

                CompositeFuture.all(futures).setHandler({
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        //sort and take top referenced functions
                        rankedOwnedFunctions = rankedOwnedFunctions.sort { a, b ->
                            return (a as JsonObject).getLong("external_reference_count") <=>
                                    (b as JsonObject).getLong("external_reference_count")
                        }.reverse() as JsonArray
                        rankedOwnedFunctions = rankedOwnedFunctions.take(topCount) as JsonArray
                        for (int i = 0; i < rankedOwnedFunctions.size(); i++) {
                            def functionId = rankedOwnedFunctions.getJsonObject(i).getString("function_id")
                            def qualifiedName = rankedOwnedFunctions.getJsonObject(i).getString("qualified_name")
                            rankedOwnedFunctions.getJsonObject(i)
                                    .put("id", functionId)
                                    .put("short_class_name", WebServices.getShortQualifiedClassName(qualifiedName))
                                    .put("class_name", WebServices.getQualifiedClassName(qualifiedName))
                                    .put("short_method_signature", WebServices.getShortMethodSignature(qualifiedName))
                                    .put("method_signature", WebServices.getMethodSignature(qualifiedName))
                                    .put("is_function", true)
                        }
                        projectRankedFunctionsCache.put(githubRepository, rankedOwnedFunctions)
                        handler.handle(Future.succeededFuture(rankedOwnedFunctions.copy()))
                    }
                })
            }
        })
    }

    @Override
    void getFunctionExternalReferences(String functionId, int offset, int limit, Handler<AsyncResult<JsonArray>> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(functionId)
                params.add(offset)
                params.add(limit)

                def conn = it.result()
                conn.queryWithParams(GET_FUNCTION_EXTERNAL_REFERENCES, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        def rs = it.result()
                        if (!rs.rows.isEmpty()) {
                            def rtnArray = new JsonArray()
                            rs.rows.each {
                                rtnArray.add(new JsonObject(it.getString("reference_data")))
                            }
                            handler.handle(Future.succeededFuture(rtnArray))
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
    void getFunctionTotalExternalReferenceCount(String functionId, Handler<AsyncResult<Long>> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(functionId)

                def conn = it.result()
                conn.queryWithParams(GET_FUNCTION_TOTAL_EXTERNAL_REFERENCE_COUNT, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        handler.handle(Future.succeededFuture(it.result().rows[0].getLong("count")))
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void addProjectImportedFile(String githubRepository, String filename, String fileId, Handler<AsyncResult> handler) {
        log.trace "Adding project imported file: $filename"
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
    void addProjectImportedFunction(String githubRepository, String functionName, String functionId, Handler<AsyncResult> handler) {
        log.trace "Adding project imported function: $functionName"
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
    void addProjectImportedDefinition(String fileId, String functionId, Handler<AsyncResult> handler) {
        log.trace "Adding project imported definition: $fileId-$functionId"
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
    void addProjectImportedReference(String fileOrFunctionId, String functionId, Handler<AsyncResult> handler) {
        log.trace "Adding project imported reference: $fileOrFunctionId-$functionId"
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
                            handler.handle(Future.succeededFuture(rs.rows as JsonArray))
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
        log.trace "Adding owner '$githubRepository' to function: $functionId"
        projectRankedFunctionsCache.invalidate(githubRepository)

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
                        getFunctionTotalExternalReferenceCount(functionId, {
                            if (it.failed()) {
                                handler.handle(Future.failedFuture(it.cause()))
                            } else {
                                redis.updateProjectReferenceLeaderboard(githubRepository, it.result(), handler)
                            }
                        })
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void removeFunctionOwner(String functionId, String qualifiedName, String githubRepository, Handler<AsyncResult> handler) {
        log.info "Removing owner '$githubRepository' from function: $functionId"
        projectRankedFunctionsCache.invalidate(githubRepository)

        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(githubRepository)
                params.add(functionId)
                params.add(qualifiedName)

                def conn = it.result()
                conn.queryWithParams(REMOVE_FUNCTION_OWNER, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        getFunctionTotalExternalReferenceCount(functionId, {
                            if (it.failed()) {
                                handler.handle(Future.failedFuture(it.cause()))
                            } else {
                                redis.updateProjectReferenceLeaderboard(githubRepository, it.result() * -1, handler)
                            }
                        })
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void getFunctionOwners(String functionId, Handler<AsyncResult<JsonArray>> handler) {
        log.trace "Getting owners of function: $functionId"
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
                            def rtnArray = new JsonArray()
                            rs.rows.each {
                                rtnArray.add(it.getString("project_name"))
                            }
                            handler.handle(Future.succeededFuture(rtnArray))
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
                params.add(fileOrFunctionReference.toString())

                def conn = it.result()
                conn.queryWithParams(ADD_FUNCTION_REFERENCE, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        getFunctionOwners(functionId, {
                            if (it.failed()) {
                                handler.handle(Future.failedFuture(it.cause()))
                            } else {
                                //update function owner in leaderboard
                                def owners = it.result() as JsonArray
                                def futures = new ArrayList<Future>()
                                for (int i = 0; i < owners.size(); i++) {
                                    def owner = owners.getString(i)
                                    def fut = Future.future()
                                    futures.add(fut)
                                    redis.updateProjectReferenceLeaderboard(owner, 1, fut.completer())
                                }
                                CompositeFuture.all(futures).setHandler(handler)
                            }
                        })
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void getProjectFileId(String project, String fileName, Handler<AsyncResult<Optional<String>>> handler) {
        log.trace "Getting project '$project' file id for file: $fileName"
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(project)
                params.add(fileName)

                def conn = it.result()
                conn.queryWithParams(GET_PROJECT_IMPORTED_FILE_ID, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        def rs = it.result()
                        if (!rs.rows.isEmpty()) {
                            def fileId = rs.rows.get(0).getString("file_id")
                            log.trace "Found function id '$fileId' for file '$fileName' in project '$project'"
                            handler.handle(Future.succeededFuture(Optional.of(fileId)))
                        } else {
                            log.trace "Could not find file id for file '$fileName' in project '$project'"
                            handler.handle(Future.succeededFuture(Optional.empty()))
                        }
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void getProjectFunctionId(String project, String functionName, Handler<AsyncResult<Optional<String>>> handler) {
        log.trace "Getting project '$project' function id for function: $functionName"
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(project)
                params.add(functionName)

                def conn = it.result()
                conn.queryWithParams(GET_PROJECT_IMPORTED_FUNCTION_ID, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        def rs = it.result()
                        if (!rs.rows.isEmpty()) {
                            def functionId = rs.rows.get(0).getString("function_id")
                            log.trace "Found function id '$functionId' for function '$functionName' in project '$project'"
                            handler.handle(Future.succeededFuture(Optional.of(functionId)))
                        } else {
                            log.trace "Could not find function id for function '$functionName' in project '$project'"
                            handler.handle(Future.succeededFuture(Optional.empty()))
                        }
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void projectHasDefinition(String fileId, String functionId, Handler<AsyncResult<Boolean>> handler) {
        log.trace "Checking project for definition between $fileId and $functionId"
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(fileId)
                params.add(functionId)

                def conn = it.result()
                conn.queryWithParams(PROJECT_HAS_DEFINITION, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        def foundDef = !it.result().rows.isEmpty()
                        if (foundDef) {
                            log.trace "Found definition between $fileId and $functionId"
                        } else {
                            log.trace "Could not find definition between $fileId and $functionId"
                        }
                        handler.handle(Future.succeededFuture(foundDef))
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void projectHasReference(String fileOrFunctionId, String functionId, Handler<AsyncResult<Boolean>> handler) {
        log.trace "Checking project for reference between $fileOrFunctionId and $functionId"
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def params = new JsonArray()
                params.add(fileOrFunctionId)
                params.add(functionId)

                def conn = it.result()
                conn.queryWithParams(PROJECT_HAS_REFERENCE, params, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        def foundRef = !it.result().rows.isEmpty()
                        if (foundRef) {
                            log.trace "Found reference between $fileOrFunctionId and $functionId"
                        } else {
                            log.trace "Could not find reference between $fileOrFunctionId and $functionId"
                        }
                        handler.handle(Future.succeededFuture(foundRef))
                    }
                    conn.close()
                })
            }
        })
    }

    @Override
    void getFunctionLeaderboard(int topCount, Handler<AsyncResult<JsonArray>> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def conn = it.result()
                conn.queryWithParams(GET_FUNCTION_LEADERBOARD, new JsonArray().add(topCount), {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        def rs = it.result()
                        if (!rs.rows.isEmpty()) {
                            def rtnArray = new JsonArray()
                            rs.rows.each {
                                rtnArray.add(it as JsonObject)
                            }
                            handler.handle(Future.succeededFuture(rtnArray))
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
    void batchImportProjectFiles(File inputFile, File outputFile, Handler<AsyncResult> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def query = BATCH_IMPORT_PROJECT_FILES
                        .replace("<inputFile>", inputFile.absolutePath)
                        .replace("<outputFile>", outputFile.absolutePath)

                def conn = it.result()
                conn.query(query, {
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
    void batchImportProjectDefinitions(File inputFile, File outputFile, Handler<AsyncResult> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def query = BATCH_IMPORT_PROJECT_DEFINITIONS
                        .replace("<inputFile>", inputFile.absolutePath)
                        .replace("<outputFile>", outputFile.absolutePath)

                def conn = it.result()
                conn.query(query, {
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
    void batchImportProjectReferences(File inputFile, File outputFile, Handler<AsyncResult> handler) {
        client.getConnection({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                def query = BATCH_IMPORT_PROJECT_REFERENCES
                        .replace("<inputFile>", inputFile.absolutePath)
                        .replace("<outputFile>", outputFile.absolutePath)

                def conn = it.result()
                conn.query(query, {
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

    boolean isBatchSupported() {
        return batchSupported
    }

}
