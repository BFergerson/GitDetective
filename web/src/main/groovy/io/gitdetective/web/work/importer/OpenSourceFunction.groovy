package io.gitdetective.web.work.importer

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class OpenSourceFunction {

    final String functionId
    final String functionDefinitionsId
    final String functionReferencesId

    OpenSourceFunction(String functionId, String functionDefinitionsId, String functionReferencesId) {
        this.functionId = functionId
        this.functionDefinitionsId = functionDefinitionsId
        this.functionReferencesId = functionReferencesId
    }

}
