package io.gitdetective.indexer.stage

import io.gitdetective.web.dao.RedisDAO
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import org.mapdb.DBMaker
import org.mapdb.Serializer

import static io.gitdetective.indexer.IndexerServices.logPrintln

/**
 * Pre-computes definition/reference counts
 * and appends data for importation by web
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class KytheIndexAugment extends AbstractVerticle {

    public static final String KYTHE_INDEX_AUGMENT = "KytheIndexAugment"
    private final static Logger log = LoggerFactory.getLogger(KytheIndexAugment.class)
    private final RedisDAO redis
    private boolean trackInternalReferences

    KytheIndexAugment(RedisDAO redis) {
        this.redis = redis
    }

    @Override
    void start() throws Exception {
        trackInternalReferences = config().getBoolean("track_internal_references")
        vertx.eventBus().consumer(KYTHE_INDEX_AUGMENT, {
            def job = (Job) it.body()
            vertx.executeBlocking({
                doAugment(job)
                it.complete()
            }, false, {})
        })
        log.info "KytheIndexAugment started"
    }

    private void doAugment(Job job) {
        logPrintln(job, "Augmenting Kythe index data")
        def dbFile = new File(job.data.getString("index_results_db"))
        def db = DBMaker
                .fileDB(dbFile)
                .fileMmapEnable()
                .make()

        def definedFunctions = db.hashSet("definedFunctions", Serializer.STRING).create()
        def outputDirectory = job.data.getString("output_directory")
        def osFunctionsOutput = new File(outputDirectory, "functions_open-source.txt")
        osFunctionsOutput.append("name|definitionCount|referenceCount\n")

        def rawFunctionDefinitions = new File(outputDirectory, "functions_definition_raw.txt")
        def rawFunctionReferences = new File(outputDirectory, "functions_reference_raw.txt")
        def neededFunctions = new HashSet<String>()
        def futures = new ArrayList<Future>()

        int lineNumber = 0
        def defLimit = config().getJsonObject("index_data_limits").getInteger("function_definitions")
        def functionDefinitions = new File(outputDirectory, "functions_definition_ready.txt")
        rawFunctionDefinitions.each {
            if (defLimit == -1 || (lineNumber - 1) < defLimit) {
                functionDefinitions.append(it)
            } else {
                return //hit definition limit
            }

            lineNumber++
            if (lineNumber > 1) {
                def lineData = it.split("\\|")
                def functionName = lineData[1]
                definedFunctions.add(functionName)

                def fut = Future.future()
                futures.add(fut)
                redis.getOpenSourceFunction(functionName, {
                    if (it.failed()) {
                        fut.fail(it.cause())
                    } else if (!it.result().isPresent()) {
                        neededFunctions.add(functionName)
                    }
                    fut.complete()
                })
            }
            functionDefinitions.append("\n")
        }

        lineNumber = 0
        def refCount = 0
        def refLimit = config().getJsonObject("index_data_limits").getInteger("function_references")
        def functionReferences = new File(outputDirectory, "functions_reference_ready.txt")
        rawFunctionReferences.each {
            lineNumber++
            if (lineNumber > 1) {
                if ((refLimit != -1 && refCount >= refLimit)) {
                    return //hit reference limit
                }
                def lineData = it.split("\\|")
                def xFunctionName = lineData[1]
                def yFunctionName = lineData[3]
                boolean internalReference = definedFunctions.contains(yFunctionName)
                if (internalReference && !trackInternalReferences) {
                    return //skip internal reference
                } else {
                    refCount++
                    functionReferences.append(it)
                }

                if (internalReference) {
                    functionReferences.append("|false")
                } else {
                    functionReferences.append("|true")
                }
                functionReferences.append("|" + yFunctionName.startsWith("kythe://jdk"))

                def fut = Future.future()
                futures.add(fut)
                redis.getOpenSourceFunction(xFunctionName, {
                    if (it.failed()) {
                        fut.fail(it.cause())
                    } else if (!it.result().isPresent()) {
                        neededFunctions.add(xFunctionName)
                    }
                    fut.complete()
                })

                def fut2 = Future.future()
                futures.add(fut2)
                redis.getOpenSourceFunction(yFunctionName, {
                    if (it.failed()) {
                        fut2.fail(it.cause())
                    } else if (!it.result().isPresent()) {
                        neededFunctions.add(yFunctionName)
                    }
                    fut2.complete()
                })
            } else {
                functionReferences.append(it) //header
            }
            functionReferences.append("\n")
        }

        CompositeFuture.all(futures).setHandler({
            if (it.failed()) {
                it.cause().printStackTrace()
                job.done(it.cause())
            } else {
                logPrintln(job, "Defining open source functions")
                def osfLimit = config().getJsonObject("index_data_limits").getInteger("functions")
                def osfCount = 0
                neededFunctions.each {
                    if (osfCount++ < osfLimit || osfLimit == -1) {
                        osFunctionsOutput.append("$it\n")
                    }
                }

                db.close()
                dbFile.delete()
                vertx.eventBus().send(GitDetectiveImportFilter.GITDETECTIVE_IMPORT_FILTER, job)
            }
        })
    }


}
