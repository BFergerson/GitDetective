package io.gitdetective.web.dao

import groovy.util.logging.Slf4j
import io.gitdetective.web.service.model.ProjectLiveReferenceTrend
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.pgclient.PgConnectOptions
import io.vertx.pgclient.PgPool
import io.vertx.sqlclient.PoolOptions
import io.vertx.sqlclient.Tuple

import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId

@Slf4j
class PostgresDAO {

    private static final ZoneId UTC = ZoneId.of("UTC")
    private static final String INSERT_FUNCTION_REFERENCE =
            'INSERT INTO function_reference (project_id, caller_function_id, callee_function_id, ' +
                    'commit_sha1, commit_date, line_number, deletion) VALUES ($1, $2, $3, $4, $5, $6, false)'
    private static final String REMOVE_FUNCTION_REFERENCE =
            'INSERT INTO function_reference (project_id, caller_function_id, callee_function_id, ' +
                    'commit_sha1, commit_date, deletion) VALUES ($1, $2, $3, $4, $5, true)'
    private static final String GET_LIVE_PROJECT_REFERENCE_TREND =
            'SELECT time_bucket(\'1 hour\', commit_date) AS one_hour, deletion, COUNT(*)\n' +
                    'FROM function_reference\n' +
                    'WHERE 1=1\n' +
                    'AND project_id = $1' +
                    'AND commit_date > NOW() - interval \'24 hours\'\n' +
                    'GROUP BY one_hour, deletion\n' +
                    'ORDER BY one_hour DESC'

    private final PgPool client

    PostgresDAO(Vertx vertx, JsonObject config) {
        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(config.getInteger("port"))
                .setHost(config.getString("host"))
                .setDatabase(config.getString("database"))
                .setUser(config.getString("user"))
                .setPassword(config.getString("password"))

        PoolOptions poolOptions = new PoolOptions().setMaxSize(5)
        client = PgPool.pool(vertx, connectOptions, poolOptions)
        client.query("SELECT 1", ar -> {
            if (ar.failed()) {
                ar.cause().printStackTrace()
                System.exit(-1)
            }
        })
    }

    PostgresDAO(PgPool client) {
        this.client = client
        client.query("SELECT 1", {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }
        })
    }

    void insertFunctionReference(String callerProjectId, String callerFunctionId, String calleeFunctionId,
                                 String callerCommitSha1, Instant callerCommitDate, int callerLineNumber,
                                 Handler<AsyncResult<Void>> handler) {
        client.preparedQuery(INSERT_FUNCTION_REFERENCE, Tuple.of(callerProjectId, callerFunctionId, calleeFunctionId,
                callerCommitSha1, OffsetDateTime.ofInstant(callerCommitDate, UTC), callerLineNumber), handler)
    }

    void removeFunctionReference(String callerProjectId, String callerFunctionId, String calleeFunctionId,
                                 String callerCommitSha1, Instant callerCommitDate,
                                 Handler<AsyncResult<Void>> handler) {
        client.preparedQuery(REMOVE_FUNCTION_REFERENCE, Tuple.of(callerProjectId, callerFunctionId, calleeFunctionId,
                callerCommitSha1, OffsetDateTime.ofInstant(callerCommitDate, UTC)), handler)
    }

    void getLiveProjectReferenceTrend(String projectId, Handler<AsyncResult<ProjectLiveReferenceTrend>> handler) {
        client.preparedQuery(GET_LIVE_PROJECT_REFERENCE_TREND, Tuple.of(projectId), {
            if (it.succeeded()) {
                def trend = new ProjectLiveReferenceTrend(projectId: projectId)
                it.result().each {
                    def time = it.getOffsetDateTime(0).toInstant()
                    def deletion = it.getBoolean(1)
                    def count = it.getLong(2)
                    trend.trendData << new ProjectLiveReferenceTrend.TrendPoint(time, deletion, count)
                }
                Collections.sort(trend.trendData)
                handler.handle(Future.succeededFuture(trend))
            } else {
                handler.handle(Future.failedFuture(it.cause()))
            }
        })
    }
}
