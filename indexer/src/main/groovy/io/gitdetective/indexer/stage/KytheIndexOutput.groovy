package io.gitdetective.indexer.stage

import io.gitdetective.indexer.stage.extract.KytheUsageExtractor
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory

import static io.gitdetective.indexer.IndexerServices.logPrintln

/**
 * Takes .kindex and outputs triples files
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class KytheIndexOutput extends AbstractVerticle {

    public static final String KYTHE_INDEX_OUTPUT = "KytheIndexOutput"
    private final static Logger log = LoggerFactory.getLogger(KytheIndexOutput.class)
    private static final File javaIndexer = new File("opt/kythe-v0.0.28/indexers/java_indexer.jar")
    private static final File dedupStreamTool = new File("opt/kythe-v0.0.28/tools/dedup_stream")
    private static final File triplesTool = new File("opt/kythe-v0.0.28/tools/triples")

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
            }, false, { res ->
                logPrintln(job, "Finished processing Kythe .kindex file(s)")
                if (outputFile.exists()) {
                    job.data.put("import_file", outputFile.absolutePath)
                    vertx.eventBus().send(KytheUsageExtractor.KYTHE_USAGE_EXTRACTOR, job)
                } else {
                    logPrintln(job, "Failed to produce Kythe index file")
                    job.done()
                }
            })
        })
        log.info "KytheIndexOutput started"
    }

    private static void processKytheIndexFile(File importFile, File outputFile) {
        ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c",
                "java -Xbootclasspath/p:" + javaIndexer.absolutePath + " com.google.devtools.kythe.analyzers.java.JavaIndexer " + importFile.absolutePath + " | "
                        + dedupStreamTool.absolutePath + " | " + triplesTool.absolutePath +
                        " >> " + outputFile.absolutePath)
        pb.inheritIO()
        pb.start().waitFor()
    }

}
