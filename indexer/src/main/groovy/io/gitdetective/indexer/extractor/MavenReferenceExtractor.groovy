package io.gitdetective.indexer.extractor

import groovy.util.logging.Slf4j
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle

import static io.gitdetective.indexer.IndexerServices.logPrintln

@Slf4j
class MavenReferenceExtractor extends AbstractVerticle {

    public static final String EXTRACTOR_ADDRESS = "MavenReferenceExtractor"

    @Override
    void start() throws Exception {
        vertx.eventBus().consumer(EXTRACTOR_ADDRESS, { msg ->
            def job = (Job) msg.body()
            logPrintln(job, "Pretend we did job")
            job.done()
        })
        log.info "MavenReferenceExtractor started"
    }
}
