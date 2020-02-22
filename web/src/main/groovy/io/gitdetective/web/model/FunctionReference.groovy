package io.gitdetective.web.model

import groovy.transform.Canonical
import io.gitdetective.web.WebServices

@Canonical
class FunctionReference {
    String projectId
    String functionId
    String commitSha1
    int lineNumber
    String kytheUri
    String qualifiedName
    String fileLocation
    String projectName
    boolean isFunction = true

    String getShortClassName() {
        return WebServices.getShortQualifiedClassName(qualifiedName)
    }

    String getClassName() {
        return WebServices.getQualifiedClassName(qualifiedName)
    }

    String getShortFunctionSignature() {
        return WebServices.getShortFunctionSignature(qualifiedName)
    }

    String getFunctionSignature() {
        return WebServices.getFunctionSignature(qualifiedName)
    }

    String getShortQualifiedFunctionName() {
        return WebServices.getShortQualifiedFunctionName(qualifiedName)
    }
}
