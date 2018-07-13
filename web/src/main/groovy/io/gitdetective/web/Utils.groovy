package io.gitdetective.web

import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory

import java.util.concurrent.TimeUnit

/**
 * Common utilities
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
final class Utils {

    private final static Logger log = LoggerFactory.getLogger(Utils.class)

    private Utils() {
    }

    //todo: extend Job and add as method to that
    static void logPrintln(Job job, String logData) {
        job.log(logData)
        log.info logData + " (repo: " + job.data.getString("github_repository") + ")"
    }

    static String getShortQualifiedClassName(String qualifiedName) {
        return getQualifiedClassName(qualifiedName).replaceAll('\\B\\w+(\\.[a-z])', '$1')
    }

    static String getQualifiedClassName(String qualifiedName) {
        def withoutArgs = qualifiedName.substring(0, qualifiedName.indexOf("("))
        try {
            return withoutArgs.substring(withoutArgs.lastIndexOf("?") + 1, withoutArgs.lastIndexOf("."))
        } catch (StringIndexOutOfBoundsException e) {
            log.error "Throwing index out of bounds on qualified name: " + qualifiedName
            throw e
        }
    }

    static String getShortMethodSignature(String qualifiedName) {
        return getMethodSignature(qualifiedName).replaceAll('\\B\\w+(\\.[a-z])', '$1')
    }

    static String getMethodSignature(String qualifiedName) {
        def withoutClassName = qualifiedName.replace(getQualifiedClassName(qualifiedName), "")
        return withoutClassName.substring(withoutClassName.substring(0, withoutClassName.indexOf("(")).lastIndexOf("?") + 2)
                .replaceAll('\\(java.lang.', "\\(").replaceAll('<java.lang.', "<")
                .replaceAll(',java.lang.', ",").replaceAll("~", "")
    }

    static boolean isValidGithubString(String str) {
        return str ==~ '[a-zA-Z0-9\\-\\_\\.]+'
    }

    static String asPrettyUptime(long uptime) {
        def days = TimeUnit.NANOSECONDS.toDays(uptime)
        if (days > 1) {
            return days + " days"
        }
        def hours = TimeUnit.NANOSECONDS.toHours(uptime)
        if (hours > 1) {
            return hours + " hours"
        }
        def minutes = TimeUnit.NANOSECONDS.toMinutes(uptime)
        if (minutes > 1) {
            return minutes + " minutes"
        }
        def seconds = TimeUnit.NANOSECONDS.toSeconds(uptime)
        if (seconds > 1) {
            return seconds + " seconds"
        } else {
            return TimeUnit.NANOSECONDS.toMillis(uptime) + " milliseconds"
        }
    }

    static String asPrettyNumber(long number) {
        if (number > 1_000_000_000) {
            return (number / 1_000_000_000d).round(2) + "B"
        } else if (number > 1_000_000) {
            return (number / 1_000_000d).round(2) + "M"
        } else if (number > 1_000) {
            return (number / 1_000d).round(2) + "K"
        } else {
            return number as String
        }
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
