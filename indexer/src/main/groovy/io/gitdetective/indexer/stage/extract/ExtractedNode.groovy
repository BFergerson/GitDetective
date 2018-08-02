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

    void addParam(int position, KytheURI identifier) {
        params[position] = identifier.toString()
    }

    String getQualifiedName(ExtractedSourceCodeUsage codeUsage) {
        if (isFile) {
            return context + identifier
        } else {
            def paramStr = ""
            if (!params.isEmpty()) {
                for (int i = 0; i < params.size(); i++) {
                    def param = params.get(i)
                    paramStr += codeUsage.paramToTypeMap.get(param)
                    if ((i + 1) < params.size()) {
                        paramStr += ","
                    }
                }
            }

            if (parentNode?.isFile && context.contains(parentNode.context)) {
                //use parent qualified name as context
                return parentNode.getQualifiedName(codeUsage) + ".$identifier($paramStr)"
            } else {
                return context + identifier + "($paramStr)"
            }
        }
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
