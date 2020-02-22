package io.gitdetective.web.service

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.google.common.base.Charsets
import com.google.common.io.Resources
import grakn.client.GraknClient
import graql.lang.Graql
import groovy.util.logging.Slf4j
import io.gitdetective.web.GitDetectiveService
import io.vertx.core.Vertx
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import org.slf4j.LoggerFactory

import static org.slf4j.Logger.ROOT_LOGGER_NAME

@Slf4j
abstract class GitDetectiveServiceTest {
    static {
        //disable grakn 'io.netty' DEBUG logging
        Logger root = (Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME)
        root.setLevel(Level.INFO)
    }

    static Vertx vertx
    static GraknClient client
    static GraknClient.Session session

    static void setUp(TestContext test) {
        String graknHost = "localhost"
        int graknPort = 48555
        String graknKeyspace = "grakn"
        client = new GraknClient("$graknHost:$graknPort")
        try {
            session = client.session(graknKeyspace)
        } catch (all) {
            all.printStackTrace()
            throw new ConnectException("Connection refused: $graknHost:$graknPort")
        }
        GitDetectiveService.setupOntology(session)

        log.info("Loading test data")
        try {
            def tx = session.transaction().write()
            tx.execute(Graql.parse(Resources.toString(Resources.getResource(
                    "test-data.gql"), Charsets.UTF_8)))
            tx.commit()
            tx.close()
        } catch (all) {
            test.fail(all)
        }
        log.info("Loaded test data")

        Async async = test.async()
        vertx = Vertx.vertx()
        async.complete()
    }

    static void tearDown(TestContext test) {
        log.info("Tearing down Vertx")
        def tx = session.transaction().write();
        tx.execute(Graql.match(Graql.var("x").isa("thing")).delete("x"))
        tx.commit()
        tx.close()
        session.close()
        client.close()
        vertx.close(test.asyncAssertSuccess())
    }
}
