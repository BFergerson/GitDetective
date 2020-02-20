package io.gitdetective.indexer.extractor

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import groovy.util.logging.Slf4j
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.eclipse.jgit.api.Git
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

import static org.slf4j.Logger.ROOT_LOGGER_NAME

@Slf4j
@RunWith(VertxUnitRunner.class)
class MavenReferenceExtractorTest {

    static {
        //disable grakn 'io.netty' DEBUG logging
        Logger root = (Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME)
        root.setLevel(Level.INFO)
    }

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
