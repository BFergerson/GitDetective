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

    void getProjectMostExternalReferencedFunctions(String githubRepository, int topCount, Handler<AsyncResult<JsonArray>> handler)

    void getFunctionExternalReferences(String functionId, int offset, int limit, Handler<AsyncResult<JsonArray>> handler)

    void getFunctionTotalExternalReferenceCount(String functionId, Handler<AsyncResult<Long>> handler)

    void addProjectImportedFile(String githubRepository, String filename, String fileId, Handler<AsyncResult> handler)

    void addProjectImportedFunction(String githubRepository, String functionName, String functionId, Handler<AsyncResult> handler)

    void addProjectImportedDefinition(String fileId, String functionId, Handler<AsyncResult> handler)

    void addProjectImportedReference(String fileOrFunctionId, String functionId, Handler<AsyncResult> handler)

    void getOwnedFunctions(String githubRepository, Handler<AsyncResult<JsonArray>> handler)

    void addFunctionOwner(String functionId, String qualifiedName, String githubRepository, Handler<AsyncResult> handler)

    void getFunctionOwners(String functionId, Handler<AsyncResult<JsonArray>> handler)

    void addFunctionReference(String functionId, JsonObject fileOrFunctionReference, Handler<AsyncResult> handler)

    void getProjectFileId(String project, String fileName, Handler<AsyncResult<Optional<String>>> handler)

    void getProjectFunctionId(String project, String functionName, Handler<AsyncResult<Optional<String>>> handler)

    void projectHasDefinition(String fileId, String functionId, Handler<AsyncResult<Boolean>> handler)

    void projectHasReference(String fileOrFunctionId, String functionId, Handler<AsyncResult<Boolean>> handler)

}
