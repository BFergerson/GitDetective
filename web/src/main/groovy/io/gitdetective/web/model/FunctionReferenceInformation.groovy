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
        return WebServices.getShortFunctionSignature(qualifiedName)
    }

    String getFunctionSignature() {
        return WebServices.getFunctionSignature(qualifiedName)
    }

    String getShortQualifiedFunctionName() {
        return WebServices.getShortQualifiedFunctionName(qualifiedName)
    }

    String getUsername() {
        if (projectName == null) {
            return null
        }
        return projectName.substring(projectName.indexOf(":") + 1, projectName.indexOf("/"))
    }

    String getUsernameAndProjectName() {
        if (projectName == null) {
            return null
        }
        return projectName.substring(projectName.indexOf(":") + 1)
    }

    String getSimpleProjectName() {
        if (projectName == null) {
            return null
        }
        return projectName.substring(projectName.indexOf("/") + 1)
    }

    String getPrettyReferenceCount() {
        return asPrettyNumber(referenceCount)
    }
}
