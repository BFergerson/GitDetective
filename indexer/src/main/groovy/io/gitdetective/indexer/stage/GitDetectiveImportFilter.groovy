package io.gitdetective.indexer.stage

import io.gitdetective.indexer.sync.IndexCacheSync
import io.gitdetective.web.dao.RedisDAO
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future

import static io.gitdetective.web.Utils.logPrintln

/**
 * Filters out definition/reference data which has already been imported
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GitDetectiveImportFilter extends AbstractVerticle {

    public static final String GITDETECTIVE_IMPORT_FILTER = "GitDetectiveImportFilter"
    private final IndexCacheSync cacheSync
    private final RedisDAO redis

    GitDetectiveImportFilter(IndexCacheSync cacheSync, RedisDAO redis) {
        this.cacheSync = cacheSync
        this.redis = redis
    }

    @Override
    void start() throws Exception {
        vertx.eventBus().consumer(GITDETECTIVE_IMPORT_FILTER, {
            def job = (Job) it.body()
            vertx.executeBlocking({
                doFilter(job)
                it.complete()
            }, false, {})
        })
    }

    private void doFilter(Job job) {
        logPrintln(job, "Filtering already imported data")

        def githubRepo = job.data.getString("github_repository")
        def outputDirectory = job.data.getString("output_directory")
        def readyFunctionDefinitions = new File(outputDirectory, "functions_definition_raw.txt")
        def readyFunctionReferences = new File(outputDirectory, "functions_reference_raw.txt")
        def funnelFutures = new ArrayList<Future>()

        def filesOutput = new File(outputDirectory, "files_raw.txt")
        def lineNumber = 0
        def filesOutputFinal = new File(outputDirectory, "files.txt")
        filesOutput.eachLine { line ->
            lineNumber++
            if (lineNumber > 1) {
                def lineData = line.split("\\|")

                def fut = Future.future()
                funnelFutures.add(fut)
                redis.getProjectImportedFileId(githubRepo, lineData[1], {
                    if (it.failed()) {
                        fut.fail(it.cause())
                    } else {
                        if (!it.result().isPresent()) {
                            filesOutputFinal.append("$line\n") //do import
                        }
                        fut.complete()
                    }
                })
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
                def fut = Future.future()
                funnelFutures.add(fut)
                def fut1 = Future.future()
                redis.getProjectImportedFileId(githubRepo, lineData[0], fut1.completer())
                def fut2 = Future.future()
                redis.getProjectImportedFunctionId(githubRepo, lineData[1], fut2.completer())

                //output final file
                CompositeFuture.all(fut1, fut2).setHandler({
                    if (it.failed()) {
                        fut.fail(it.cause())
                    } else {
                        def ids = (it.result().list() as List<Optional<String>>)
                        if (ids.get(0).isPresent() && ids.get(1).isPresent()) {
                            //check if import needed
                            if (!cacheSync.hasDefinition(ids.get(0).get(), ids.get(1).get())) {
                                functionDefinitionsFinal.append("$line\n") //do import
                            }
                        } else {
                            functionDefinitionsFinal.append("$line\n") //do import
                        }
                        fut.complete()
                    }
                })
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
                def fut = Future.future()
                funnelFutures.add(fut)
                def fut1 = Future.future()
                if (lineData[0].contains("#")) {
                    redis.getProjectImportedFunctionId(githubRepo, lineData[0], fut1.completer())
                } else {
                    redis.getProjectImportedFileId(githubRepo, lineData[0], fut1.completer())
                }
                def fut2 = Future.future()
                redis.getProjectImportedFunctionId(githubRepo, lineData[1], fut2.completer())

                //output final file
                CompositeFuture.all(fut1, fut2).setHandler({
                    if (it.failed()) {
                        fut.fail(it.cause())
                    } else {
                        def ids = (it.result().list() as List<Optional<String>>)
                        if (ids.get(0).isPresent() && ids.get(1).isPresent()) {
                            //check if import needed
                            if (!cacheSync.hasReference(ids.get(0).get(), ids.get(1).get())) {
                                functionReferencesFinal.append("$line\n") //do import
                            }
                        } else {
                            functionReferencesFinal.append("$line\n") //do import
                        }
                        fut.complete()
                    }
                })
            } else {
                functionReferencesFinal.append("$line\n") //header
            }
        }

        CompositeFuture.all(funnelFutures).setHandler({
            if (it.failed()) {
                it.cause().printStackTrace()
                logPrintln(job, it.cause().getMessage())
                job.done(it.cause())
            } else {
                vertx.eventBus().send(KytheIndexAugment.KYTHE_INDEX_AUGMENT, job)
            }
        })
    }

}
