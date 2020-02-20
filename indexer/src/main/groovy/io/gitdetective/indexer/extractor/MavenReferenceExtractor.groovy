package io.gitdetective.indexer.extractor

import com.codebrig.phenomena.Phenomena
import com.codebrig.phenomena.code.CodeObserverVisitor
import com.codebrig.phenomenon.kythe.KytheIndexObserver
import com.codebrig.phenomenon.kythe.build.KytheIndexBuilder
import com.codebrig.phenomenon.kythe.observe.KytheRefCallObserver
import groovy.util.logging.Slf4j
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle

import java.util.stream.Collectors

import static io.gitdetective.indexer.IndexerServices.logPrintln

@Slf4j
class MavenReferenceExtractor extends AbstractVerticle {

    public static final String EXTRACTOR_ADDRESS = "MavenReferenceExtractor"

    @Override
    void start() throws Exception {
        vertx.eventBus().consumer(EXTRACTOR_ADDRESS, { msg ->
            def job = (Job) msg.body()
            extractProjectReferences(job)
            logPrintln(job, "Pretend we did job")
            job.done()
        })
        log.info "MavenReferenceExtractor started"
    }

    static void extractProjectReferences(Job job) {
        def outDir = new File(job.data.getString("output_directory"))
        def kytheObservers = new ArrayList<KytheIndexObserver>()
        def refCallObserver = new KytheRefCallObserver()
        kytheObservers.add(refCallObserver)
        def index = new KytheIndexBuilder(outDir)
                .setKytheOutputDirectory(outDir)
                .setKytheDirectory(new File("opt/kythe-v0.0.28"))
                .build(kytheObservers)

        def phenomena = new Phenomena()
        phenomena.scanPath = new ArrayList<>()
        phenomena.scanPath.add(outDir.absolutePath)
        def visitor = new CodeObserverVisitor()
        visitor.addObservers(kytheObservers)
        phenomena.setupVisitor(visitor)
        phenomena.connectToBabelfish()
        phenomena.processScanPath().collect(Collectors.toList())
        phenomena.close()
    }
}
