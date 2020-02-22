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
        return WebServices.getShortFunctionSignature(qualifiedName)
    }

    String getFunctionSignature() {
        return WebServices.getFunctionSignature(qualifiedName)
    }

    String getShortQualifiedFunctionName() {
        return WebServices.getShortQualifiedFunctionName(qualifiedName)
    }

    @Override
    String toString() {
        return qualifiedName
    }
}
