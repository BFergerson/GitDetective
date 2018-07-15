package io.gitdetective.indexer

import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory

import java.util.concurrent.TimeUnit

/**
 * Services and common utility methods available in Indexer module
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
final class IndexerServices {

    private final static Logger log = LoggerFactory.getLogger(IndexerServices.class)

    private IndexerServices() {
    }

    static void logPrintln(Job job, String logData) {
        job.log(logData)
        log.info logData + " (repo: " + job.data.getString("github_repository") + ")"
    }

    static String asPrettyTime(long ns) {
        double tookTimeMs = TimeUnit.NANOSECONDS.toMillis(ns)
        if (tookTimeMs > 1000 * 60) {
            return String.format("%.2f", (tookTimeMs / (1000.00d * 60.00d))) + "mins"
        } else if (tookTimeMs > 1000) {
            return String.format("%.2f", (tookTimeMs / 1000.00d)) + "secs"
        } else {
            return tookTimeMs + "ms"
        }
    }

    static MessageCodec messageCodec(final Class type) {
        return new MessageCodec() {
            void encodeToWire(Buffer buffer, Object o) {
                throw new UnsupportedOperationException("Not supported yet.")
            }

            Object decodeFromWire(int pos, Buffer buffer) {
                throw new UnsupportedOperationException("Not supported yet.")
            }

            Object transform(Object o) {
                return o
            }

            String name() {
                return type.getSimpleName()
            }

            byte systemCodecID() {
                return -1
            }
        }
    }

}
