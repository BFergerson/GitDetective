package io.gitdetective.indexer.stage.extract

import com.google.devtools.kythe.util.KytheURI
import groovy.transform.Canonical

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
@Canonical
class ExtractedNode {
    boolean isFile
    boolean isFunction
    KytheURI uri
    String signatureSalt
    String context
    String identifier
    ExtractedNode parentNode
    final List<String> params = new ArrayList<>()

    void addParam(int position, String identifier) {
        params[position] = identifier
    }

    String getQualifiedName() {
        if (context == null || identifier == null) {
            println "here" //todo: here
        }
        if (!params.isEmpty()) {
            println "here" //todo: here
        }
        return context + identifier + "()"
    }

    @Override
    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false
        ExtractedNode that = (ExtractedNode) o
        if (signatureSalt != that.signatureSalt) return false
        return true
    }

    @Override
    int hashCode() {
        return (signatureSalt != null ? signatureSalt.hashCode() : 0)
    }

}
