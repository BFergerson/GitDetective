package io.gitdetective.web.service.model

import groovy.transform.Canonical
import io.gitdetective.web.WebServices

@Canonical
class FunctionReferenceInformation {
    String kytheUri
    String qualifiedName
    int referenceCount

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
}
