package io.gitdetective.web.service.model

import groovy.transform.Canonical

@Canonical
class FunctionReferenceInformation {
    String functionName
    String kytheUri
    int referenceCount
}
