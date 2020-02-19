package io.gitdetective.indexer.extractor

import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.eclipse.jgit.api.Git
import org.junit.Test
import org.junit.runner.RunWith

import java.util.concurrent.TimeUnit

@RunWith(VertxUnitRunner.class)
class MavenReferenceExtractorTest {

    private static final Logger logger = LoggerFactory.getLogger(MavenReferenceExtractorTest.class)

    @Test(timeout = 30000L)
    void testOtherProject() {
        def outDir = new File("/tmp/stuff")
        if (outDir.exists()) {
            outDir.deleteDir()
        }

        Git.cloneRepository()
                .setURI("https://github.com/bfergerson/otherproject.git")
                .setDirectory(outDir)
                .setCloneSubmodules(true)
                .setTimeout(TimeUnit.MINUTES.toSeconds(5) as int)
                .call()

        Job job = new Job("maven-test", new JsonObject([
                "output_directory": outDir.absolutePath
        ]))
        MavenReferenceExtractor.extractProjectReferences(job)
        println "done"
    }
}
