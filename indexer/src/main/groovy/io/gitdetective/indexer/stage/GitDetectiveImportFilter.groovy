package io.gitdetective.indexer.stage

import io.gitdetective.indexer.sync.IndexCacheSync
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle

import static io.gitdetective.web.Utils.logPrintln

/**
 * Filters out definition/reference data which has already been imported
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GitDetectiveImportFilter extends AbstractVerticle {

    public static final String GITDETECTIVE_IMPORT_FILTER = "GitDetectiveImportFilter"
    private final IndexCacheSync cacheSync

    GitDetectiveImportFilter(IndexCacheSync cacheSync) {
        this.cacheSync = cacheSync
    }

    @Override
    void start() throws Exception {
        vertx.eventBus().consumer(GITDETECTIVE_IMPORT_FILTER, {
            def job = (Job) it.body()
            vertx.executeBlocking({
                doFilter(job)
                it.complete()
            }, false, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                    logPrintln(job, it.cause().getMessage())
                    job.done(it.cause())
                } else {
                    vertx.eventBus().send(KytheIndexAugment.KYTHE_INDEX_AUGMENT, job)
                }
            })
        })
    }

    private void doFilter(Job job) {
        logPrintln(job, "Filtering already imported data")

        def githubRepo = job.data.getString("github_repository")
        def outputDirectory = job.data.getString("output_directory")
        def readyFunctionDefinitions = new File(outputDirectory, "functions_definition_raw.txt")
        def readyFunctionReferences = new File(outputDirectory, "functions_reference_raw.txt")

        def filesOutput = new File(outputDirectory, "files_raw.txt")
        def lineNumber = 0
        def filesOutputFinal = new File(outputDirectory, "files.txt")
        filesOutput.eachLine { line ->
            lineNumber++
            if (lineNumber > 1) {
                def lineData = line.split("\\|")

                def existingFile = cacheSync.getProjectFileId(githubRepo, lineData[1])
                if (!existingFile.isPresent()) {
                    filesOutputFinal.append("$line\n") //do import
                }
            } else {
                filesOutputFinal.append("$line\n") //header
            }
        }

        lineNumber = 0
        def functionDefinitionsFinal = new File(outputDirectory, "functions_definition_ready.txt")
        readyFunctionDefinitions.eachLine { line ->
            lineNumber++
            if (lineNumber > 1) {
                def lineData = line.split("\\|")

                //replace everything with ids (if possible)
                def existingFile = cacheSync.getProjectFileId(githubRepo, lineData[0])
                def existingFunction = cacheSync.getProjectFunctionId(githubRepo, lineData[1])

                if (existingFile.isPresent() && existingFunction.isPresent()) {
                    //check if import needed
                    if (!cacheSync.hasDefinition(existingFile.get(), existingFunction.get())) {
                        functionDefinitionsFinal.append("$line\n") //do import
                    }
                } else {
                    functionDefinitionsFinal.append("$line\n") //do import
                }
            } else {
                functionDefinitionsFinal.append("$line\n") //header
            }
        }

        lineNumber = 0
        def functionReferencesFinal = new File(outputDirectory, "functions_reference_ready.txt")
        readyFunctionReferences.eachLine { line ->
            lineNumber++
            if (lineNumber > 1) {
                def lineData = line.split("\\|")

                //replace everything with ids (if possible)
                def existingFileOrFunction
                if (lineData[0].contains("#")) {
                    existingFileOrFunction = cacheSync.getProjectFunctionId(githubRepo, lineData[0])
                } else {
                    existingFileOrFunction = cacheSync.getProjectFileId(githubRepo, lineData[0])
                }
                def existingFunction = cacheSync.getProjectFunctionId(githubRepo, lineData[1])

                if (existingFileOrFunction.isPresent() && existingFunction.isPresent()) {
                    //check if import needed
                    if (!cacheSync.hasReference(existingFileOrFunction.get(), existingFunction.get())) {
                        functionReferencesFinal.append("$line\n") //do import
                    }
                } else {
                    functionReferencesFinal.append("$line\n") //do import
                }
            } else {
                functionReferencesFinal.append("$line\n") //header
            }
        }
    }

}
