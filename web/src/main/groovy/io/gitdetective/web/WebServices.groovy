package io.gitdetective.web

import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory

import java.text.DecimalFormat
import java.util.concurrent.TimeUnit

/**
 * Services and common utility methods available in Web module
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
final class WebServices {

    static final String CREATE_JOB = "CreateJob"
    static final String GET_ACTIVE_JOBS = "GetActiveJobs"
    static final String GET_TRIGGER_INFORMATION = "GetTriggerInformation"
    static final String GET_LATEST_JOB_LOG = "GetLatestJobLog"
    static final String GET_FUNCTION_EXTERNAL_REFERENCES = "GetFunctionExternalReferences"
    static final String GET_PROJECT_MOST_REFERENCED_FUNCTIONS = "GetProjectMostReferencedMethods"
    static final String GET_PROJECT_REFERENCE_LEADERBOARD = "GetProjectReferenceLeaderboard"
    static final String GET_PROJECT_FILE_COUNT = "GetProjectFileCount"
    static final String GET_PROJECT_METHOD_INSTANCE_COUNT = "GetProjectFunctionInstanceCount"
    static final String GET_PROJECT_FIRST_INDEXED = "GetProjectFirstIndexed"
    static final String GET_PROJECT_LAST_INDEXED = "GetProjectLastIndexed"
    static final String GET_PROJECT_LAST_INDEXED_COMMIT_INFORMATION = "GetProjectLastIndexedCommitInformation"
    private final static Logger log = LoggerFactory.getLogger(WebServices.class)
    private final static DecimalFormat decimalFormat = new DecimalFormat("#.00")

    private WebServices() {
    }

    static void logPrintln(Job job, String logData) {
        job.log(logData)
        log.info logData + " (repo: " + job.data.getString("github_repository") + ")"
    }

    static String getShortQualifiedClassName(String qualifiedName) {
        return getQualifiedClassName(qualifiedName).replaceAll('\\B\\w+(\\.[a-z])', '$1')
    }

    static String getShortQualifiedMethodName(String qualifiedName) {
        return getShortQualifiedClassName(qualifiedName) + "." + getShortMethodSignature(qualifiedName)
    }

    static String getQualifiedClassName(String qualifiedName) {
        def withoutArgs = qualifiedName.substring(0, qualifiedName.indexOf("("))
        if (withoutArgs.contains("<")) {
            withoutArgs = withoutArgs.substring(0, withoutArgs.indexOf("<"))
            return withoutArgs.substring(withoutArgs.lastIndexOf("?") + 1, withoutArgs.lastIndexOf("."))
        } else {
            return withoutArgs.substring(withoutArgs.lastIndexOf("?") + 1, withoutArgs.lastIndexOf("."))
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
            return decimalFormat.format(number / 1_000_000_000d) + "B"
        } else if (number > 1_000_000) {
            return decimalFormat.format(number / 1_000_000d) + "M"
        } else if (number > 1_000) {
            return decimalFormat.format(number / 1_000d) + "K"
        } else {
            return number as String
        }
    }

    static String asPrettyTime(long ns) {
        double tookTimeMs = TimeUnit.NANOSECONDS.toMillis(ns)
        if (tookTimeMs > 1000 * 60) {
            return decimalFormat.format(tookTimeMs / (1000.00d * 60.00d)) + "mins"
        } else if (tookTimeMs > 1000) {
            return decimalFormat.format(tookTimeMs / 1000.00d) + "secs"
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
