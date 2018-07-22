package io.gitdetective.indexer.stage.extract

import groovy.transform.Canonical
import io.vertx.core.json.JsonObject

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
@Canonical
class ExtractedSourceCodeUsage {
    File importFile
    String buildDirectory
    Map<String, String> qualifiedNameMap
    Map<String, String> classToSourceMap
    Map<String, String> aliasMap
    Map<String, int[]> sourceLocationMap
    Set<String> functionNameSet
    long fileCount
    JsonObject indexDataLimits
}
