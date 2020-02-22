package io.gitdetective.web.model

import groovy.transform.Canonical
import io.gitdetective.web.WebServices

@Canonical
class FunctionInformation {
    String kytheUri
    String qualifiedName

    String getShortClassName() {
        return WebServices.getShortQualifiedClassName(qualifiedName)
    }

    String getClassName() {
        return WebServices.getQualifiedClassName(qualifiedName)
    }

    String getShortFunctionSignature() {
        return WebServices.getShortMethodSignature(qualifiedName)
    }

    String getFunctionSignature() {
        return WebServices.getMethodSignature(qualifiedName)
    }

    String getShortQualifiedFunctionName() {
        return WebServices.getShortQualifiedMethodName(qualifiedName)
    }
}
