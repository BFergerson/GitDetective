package io.gitdetective.web.work.importer

import ai.grakn.graql.Query
import groovy.transform.Canonical

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
@Canonical
class ImportableSourceCode {
    Query insertQuery
    String filename
    String fileLocation
    String fileId
    String fileQualifiedName
    String functionName
    String functionQualifiedName
    String functionId
    String functionInstanceId
    String referenceFunctionId
    String referenceFunctionInstanceId
    boolean isFileReferencing
    boolean isExternalReference
}
