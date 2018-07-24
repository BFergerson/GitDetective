package io.gitdetective.web.work.importer

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class ImportSessionData {
    final Map<String, OpenSourceFunction> openSourceFunctions = new HashMap<>()
    final Map<String, String> importedFiles = new HashMap<>()
    final Map<String, String> definedFunctions = new HashMap<>()
    final Map<String, String> definedFunctionInstances = new HashMap<>()
}
