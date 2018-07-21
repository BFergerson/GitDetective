package io.gitdetective.indexer.stage.extract

import com.google.devtools.kythe.util.KytheURI
import groovy.transform.Canonical
import io.vertx.core.json.JsonObject
import org.apache.commons.io.FilenameUtils
import org.gradle.internal.impldep.com.google.common.collect.Maps

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
@Canonical
class ExtractedSourceCodeUsage {
    File importFile
    String buildDirectory
    final Map<String, String> bindings = Maps.newConcurrentMap()
    final Map<String, ExtractedNode> extractedNodes = Maps.newConcurrentMap()
    Map<String, String> paramToTypeMap
    Map<String, int[]> sourceLocationMap
    Set<String> functionNameSet
    long fileCount
    JsonObject indexDataLimits

    ExtractedNode getBindedNode(KytheURI uri) {
        def bindingStr = bindings.get(uri.signature, uri.toString())
        return getExtractedNode(KytheURI.parse(bindingStr))
    }

    void addBinding(KytheURI subjectUri, KytheURI objectUri) {
        if (subjectUri.signature == "" || objectUri.signature == "") {
            throw new IllegalStateException("Didn't expect this")
        }
        bindings.put(subjectUri.signature, objectUri.toString())
    }

    ExtractedNode getParentNode(KytheURI uri) {
        def node = extractedNodes.get(uri.toString())
        if (node == null) {
            node = extractedNodes.get(uri.signature)
        }
        if (node != null) {
            if (node.isFile || node.isFunction || node.parentNode == null) {
                return node
            } else if (node.parentNode.uri != null) {
                return getParentNode(node.parentNode.uri)
            } else {
                return node.parentNode
            }
        }
        return null
    }

    ExtractedNode getExtractedNode(KytheURI uri) {
        if (uri.signature == null || uri.signature.isEmpty()) {
            //file
            def file = new ExtractedNode()
            extractedNodes.putIfAbsent(uri.toString(), file)
            return extractedNodes.get(uri.toString())
        }

        //function
        def function = new ExtractedNode()
        function.signatureSalt = uri.signature
        extractedNodes.putIfAbsent(uri.signature, function)
        return extractedNodes.get(uri.signature)
    }

    String getQualifiedName(KytheURI uri) {
        if (uri.signature == null || uri.signature.isEmpty()) {
            //class qualified name
            return FilenameUtils.removeExtension(uri.path.replace("src/main/" + uri.path.substring(
                    uri.path.lastIndexOf(".") + 1), "").substring(1))
                    .replace("/", ".") //todo: better? only handles src/main/*language*
        }

        //function qualified name
        return extractedNodes.get(uri.signature).qualifiedName
    }

    String getFunctionQualifiedName(String signature) {
        return extractedNodes.get(signature).qualifiedName
    }

}
