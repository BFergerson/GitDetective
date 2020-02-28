package io.gitdetective.indexer.extractor

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import groovy.util.logging.Slf4j
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.PubSecKeyOptions
import io.vertx.ext.auth.jwt.JWTAuth
import io.vertx.ext.auth.jwt.JWTAuthOptions
import io.vertx.ext.unit.TestContext
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
    void testOtherProject(TestContext test) {
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
                "output_directory" : outDir.absolutePath,
                "github_repository": "bfergerson/otherproject",
                "commit": "b0be5053300ef5baabe7706f1cb440e38aa55565",
                "commit_date": "2020-02-22T20:02:20Z"
        ]))

        def indexerConfig = new JsonObject(new File("indexer-config.json").text)
        def vertx = Vertx.vertx()
        def provider = JWTAuth.create(vertx, new JWTAuthOptions()
                .addPubSecKey(new PubSecKeyOptions()
                        .setAlgorithm("HS256")
                        .setPublicKey(indexerConfig.getString("gitdetective_service.api_key"))
                        .setSymmetric(true)))
        def apiKey = provider.generateToken(new JsonObject())

        def async = test.async()
        MavenReferenceExtractor.extractProjectReferences(job, vertx,indexerConfig, apiKey, {
            if (it.succeeded()) {
                async.complete()
            } else {
                test.fail(it.cause())
            }
        })
    }

//    @Test(timeout = 30000L)
//    void testMyProject(TestContext test) {
//        def outDir = new File("/tmp/stuff")
//        if (outDir.exists()) {
//            outDir.deleteDir()
//        }
//
//        Git.cloneRepository()
//                .setURI("https://github.com/bfergerson/myproject.git")
//                .setDirectory(outDir)
//                .setCloneSubmodules(true)
//                .setTimeout(TimeUnit.MINUTES.toSeconds(5) as int)
//                .call()
//
//        Job job = new Job("maven-test", new JsonObject([
//                "output_directory" : outDir.absolutePath,
//                "github_repository": "bfergerson/myproject",
//                "commit": "28ea7da14fcf03658721e05a985acaa2b86c8bd5",
//                "commit_date": "2020-02-22T20:02:20Z"
//        ]))
//
//        def indexerConfig = new JsonObject(new File("indexer-config.json").text)
//        def vertx = Vertx.vertx()
//        def provider = JWTAuth.create(vertx, new JWTAuthOptions()
//                .addPubSecKey(new PubSecKeyOptions()
//                        .setAlgorithm("HS256")
//                        .setPublicKey(indexerConfig.getString("gitdetective_service.api_key"))
//                        .setSymmetric(true)))
//        def apiKey = provider.generateToken(new JsonObject())
//
//        def async = test.async()
//        MavenReferenceExtractor.extractProjectReferences(job, vertx,indexerConfig, apiKey, {
//            if (it.succeeded()) {
//                async.complete()
//            } else {
//                test.fail(it.cause())
//            }
//        })
//    }
}
