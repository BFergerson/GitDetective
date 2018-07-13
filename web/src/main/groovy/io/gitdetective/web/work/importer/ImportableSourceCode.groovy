package io.gitdetective.web.work.importer

import ai.grakn.graql.Query
import groovy.transform.Canonical

@Canonical
class ImportableSourceCode {
    Query insertQuery
    String filename
    String fileId
    String functionName
    String functionId
    String functionInstanceId
    String referenceFunctionId
    String referenceFunctionInstanceId
    boolean isFileReferencing
    boolean isExternalReference
}
