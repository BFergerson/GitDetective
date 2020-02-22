package io.gitdetective.web.model

import groovy.transform.Canonical
import io.gitdetective.web.WebServices

import static io.gitdetective.web.WebServices.asPrettyNumber

@Canonical
class FunctionReferenceInformation {
    String functionId
    String kytheUri
    String qualifiedName
    int referenceCount
    String projectName = null

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

    String getUsername() {
        return projectName.substring(projectName.indexOf(":") + 1, projectName.indexOf("/"))
    }

    String getUsernameAndProjectName() {
        return projectName.substring(projectName.indexOf(":") + 1)
    }

    String getSimpleProjectName() {
        return projectName.substring(projectName.indexOf("/") + 1)
    }

    String getPrettyReferenceCount() {
        return asPrettyNumber(referenceCount)
    }
}
