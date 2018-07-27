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
    final Map<String, ExtractedNode> extractedNodes = Maps.newConcurrentMap()
    File importFile
    String buildDirectory
    Map<String, String> bindings
    Map<String, String> paramToTypeMap
    Map<String, String> fileLocations
    Map<String, String> aliasMap
    Map<String, int[]> sourceLocationMap
    Set<String> functionNameSet
    Set<String> definedFiles
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
            return getQualifiedName(uri, true)
        } else {
            return getQualifiedName(uri, false)
        }
    }

    String getQualifiedName(KytheURI uri, boolean isClass) {
        if (isClass) {
            def qualifiedName = FilenameUtils.removeExtension(uri.path.replace("src/main/" + uri.path.substring(
                    uri.path.lastIndexOf(".") + 1) + "/", ""))
                    .replace("/", ".")
            if (qualifiedName.contains("#")) {
                return qualifiedName.substring(0, qualifiedName.indexOf("#"))
            } else {
                return qualifiedName
            }
        } else {
            return extractedNodes.get(uri.signature).getQualifiedName(this)
        }
    }

}
