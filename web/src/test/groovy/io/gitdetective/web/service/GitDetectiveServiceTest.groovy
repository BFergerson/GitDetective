package io.gitdetective.web.service

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.google.common.base.Charsets
import com.google.common.io.Resources
import grakn.client.GraknClient
import graql.lang.Graql
import groovy.util.logging.Slf4j
import io.gitdetective.web.GitDetectiveService
import io.gitdetective.web.dao.PostgresDAO
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.ext.unit.TestContext
import io.vertx.pgclient.PgConnectOptions
import io.vertx.pgclient.PgPool
import io.vertx.sqlclient.PoolOptions
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
    static GraknClient graknClient
    static GraknClient.Session graknSession
    static PgPool postgresClient
    static PostgresDAO postgres

    static void setUp(Handler<AsyncResult<Void>> handler) {
        String graknHost = "localhost"
        int graknPort = 48555
        String graknKeyspace = "grakn"
        graknClient = new GraknClient("$graknHost:$graknPort")
        try {
            graknSession = graknClient.session(graknKeyspace)
        } catch (all) {
            handler.handle(Future.failedFuture(all.cause))
        }
        GitDetectiveService.setupOntology(graknSession)

        log.info("Loading test data")
        try {
            def tx = graknSession.transaction().write()
            tx.execute(Graql.parse(Resources.toString(Resources.getResource(
                    "test-data.gql"), Charsets.UTF_8)))
            tx.commit()
            tx.close()
        } catch (all) {
            handler.handle(Future.failedFuture(all))
        }
        log.info("Loaded test data")

        vertx = Vertx.vertx()

        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(5432)
                .setHost("localhost")
                .setDatabase("postgres")
                .setUser("postgres")
                .setPassword("postgres")
        PoolOptions poolOptions = new PoolOptions().setMaxSize(5)
        postgresClient = PgPool.pool(vertx, connectOptions, poolOptions)
        postgresClient.query("SELECT 1 FROM information_schema.tables WHERE table_name = 'function_reference'", {
            if (it.succeeded()) {
                if (it.result().isEmpty()) {
                    postgresClient.query(Resources.toString(Resources.getResource(
                            "reference-storage-schema.sql"), Charsets.UTF_8), {
                        if (it.succeeded()) {
                            postgres = new PostgresDAO(postgresClient)
                            handler.handle(Future.succeededFuture())
                        } else {
                            handler.handle(Future.failedFuture(it.cause()))
                        }
                    })
                } else {
                    postgres = new PostgresDAO(postgresClient)
                    handler.handle(Future.succeededFuture())
                }
            } else {
                handler.handle(Future.failedFuture(it.cause()))
            }
        })
    }

    static void tearDown(TestContext test) {
        log.info("Tearing down Vertx")
        def tx = graknSession.transaction().write();
        tx.execute(Graql.match(Graql.var("x").isa("thing")).delete("x"))
        tx.commit()
        tx.close()
        graknSession.close()
        graknClient.close()
        postgresClient.close()
        vertx.close(test.asyncAssertSuccess())
    }
}
