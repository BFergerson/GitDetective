package io.gitdetective.web.dao.storage

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * todo: description
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
interface ReferenceStorage {

    void getProjectMostExternalReferencedMethods(String githubRepository, int topCount, Handler<AsyncResult<JsonArray>> handler)

    void getMethodExternalReferences(String functionId, int offset, int limit, Handler<AsyncResult<JsonArray>> handler)

    void getFunctionTotalExternalReferenceCount(String functionId, Handler<AsyncResult<Long>> handler)

    void getProjectReferenceLeaderboard(int topCount, Handler<AsyncResult<JsonArray>> handler)

    void cacheProjectImportedFile(String githubRepository, String filename, String fileId, Handler<AsyncResult> handler)

    void cacheProjectImportedFunction(String githubRepository, String functionName, String functionId, Handler<AsyncResult> handler)

    void cacheProjectImportedDefinition(String fileId, String functionId, Handler<AsyncResult> handler)

    void cacheProjectImportedReference(String fileOrFunctionId, String functionId, Handler<AsyncResult> handler)

    void getOwnedFunctions(String githubRepository, Handler<AsyncResult<JsonArray>> handler)

    void addFunctionOwner(String functionId, String qualifiedName, String githubRepository, Handler<AsyncResult> handler)

    void getFunctionOwners(String functionId, Handler<AsyncResult<JsonArray>> handler)

    void addFunctionReference(String functionId, JsonObject fileOrFunctionReference, Handler<AsyncResult> handler)

}