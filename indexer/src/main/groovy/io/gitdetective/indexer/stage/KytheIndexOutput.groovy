package io.gitdetective.indexer.stage

import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject

import static io.gitdetective.web.Utils.logPrintln

/**
 * Takes .kindex and outputs triples files
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class KytheIndexOutput extends AbstractVerticle {

    public static final String KYTHE_INDEX_OUTPUT = "KytheIndexOutput"
    private static final File javaIndexer = new File("opt/kythe-v0.0.26/indexers/java_indexer.jar")
    private static final File dedupStreamTool = new File("opt/kythe-v0.0.26/tools/dedup_stream")
    private static final File triplesTool = new File("opt/kythe-v0.0.26/tools/triples")

    @Override
    void start() throws Exception {
        vertx.eventBus().consumer(KYTHE_INDEX_OUTPUT, {
            def job = (Job) it.body()
            def kytheOutputDir = new File(job.data.getString("output_directory"), "kythe")
            def outputFile = new File(job.data.getString("output_directory"), UUID.randomUUID().toString() + ".txt")

            vertx.executeBlocking({ future ->
                logPrintln(job, "Processing Kythe .kindex file(s)")
                kytheOutputDir.listFiles().each {
                    processKytheIndexFile(it, outputFile)
                }
                future.complete()
            }, { res ->
                logPrintln(job, "Finished processing Kythe .kindex file(s)")
                if (outputFile.exists()) {
                    job.data.put("import_file", outputFile.absolutePath)
                    job.save().setHandler({
                        if (it.failed()) {
                            it.cause().printStackTrace()
                        } else {
                            job = it.result()
                            vertx.eventBus().send(KytheUsageExtractor.KYTHE_USAGE_EXTRACTOR, job)
                        }
                    })
                } else {
                    logPrintln(job, "Failed to produce Kythe index file")
                    job.done()
                }
            })
        })
    }

    private static void processKytheIndexFile(File importFile, File outputFile) {
        ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c",
                "java -jar " + javaIndexer.absolutePath + " " + importFile.absolutePath + " | "
                        + dedupStreamTool.absolutePath + " | " + triplesTool.absolutePath +
                        " >> " + outputFile.absolutePath)
        pb.inheritIO()
        pb.start().waitFor()
    }

}
