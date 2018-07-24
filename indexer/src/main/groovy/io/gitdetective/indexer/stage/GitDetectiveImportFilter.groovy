package io.gitdetective.indexer.stage

import io.gitdetective.indexer.cache.ProjectDataCache
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory

import static io.gitdetective.indexer.IndexerServices.logPrintln

/**
 * Filters out definition/reference data which has already been imported
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GitDetectiveImportFilter extends AbstractVerticle {

    public static final String GITDETECTIVE_IMPORT_FILTER = "GitDetectiveImportFilter"
    private final static Logger log = LoggerFactory.getLogger(GitDetectiveImportFilter.class)
    private final ProjectDataCache projectCache

    GitDetectiveImportFilter(ProjectDataCache projectCache) {
        this.projectCache = projectCache
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
        log.info "GitDetectiveImportFilter started"
    }

    private void doFilter(Job job) {
        logPrintln(job, "Filtering already imported data")

        def githubRepository = job.data.getString("github_repository")
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

                def existingFile = projectCache.getProjectFileId(githubRepository, lineData[1])
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
                def existingFile = projectCache.getProjectFileId(githubRepository, lineData[0])
                def existingFunction = projectCache.getProjectFunctionId(githubRepository, lineData[1])

                if (existingFile.isPresent() && existingFunction.isPresent()) {
                    //check if import needed
                    if (!projectCache.hasDefinition(existingFile.get(), existingFunction.get())) {
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
                if (lineData[1].contains("#")) {
                    existingFileOrFunction = projectCache.getProjectFunctionId(githubRepository, lineData[1])
                } else {
                    existingFileOrFunction = projectCache.getProjectFileId(githubRepository, lineData[1])
                }
                def existingFunction = projectCache.getProjectFunctionId(githubRepository, lineData[3])

                if (existingFileOrFunction.isPresent() && existingFunction.isPresent()) {
                    //check if import needed
                    if (!projectCache.hasReference(existingFileOrFunction.get(), existingFunction.get())) {
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
