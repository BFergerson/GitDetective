package io.gitdetective.indexer.stage

import com.google.common.collect.Sets
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import org.mapdb.DBMaker
import org.mapdb.Serializer

import static io.gitdetective.web.Utils.logPrintln

/**
 * Takes Kythe triples and extracts usage triples (ref/def)
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class KytheUsageExtractor extends AbstractVerticle {

    public static final String KYTHE_USAGE_EXTRACTOR = "KytheUsageExtractor"
    private final static Logger log = LoggerFactory.getLogger(KytheUsageExtractor.class)
    private static final Set<String> KYTHE_PARSE_SET = Sets.newHashSet(
            "/kythe/node/kind", "/kythe/edge/childof",
            "/kythe/edge/ref/call", "/kythe/edge/defines",
            "/kythe/loc/start", "/kythe/loc/end")

    @Override
    void start() throws Exception {
        vertx.eventBus().consumer(KYTHE_USAGE_EXTRACTOR, {
            def job = (Job) it.body()
            try {
                processImportFile(job, new File(job.data.getString("import_file")))
            } catch (Exception ex) {
                ex.printStackTrace()
                logPrintln(job, ex.getMessage())
                job.done(ex)
            }
        })
        log.info "KytheUsageExtractor started"
    }

    private void processImportFile(Job job, File importFile) {
        logPrintln(job, "Analyzing Kythe .kindex file")
        def outputDirectory = job.data.getString("output_directory")
        def filesOutput = new File(outputDirectory, "files_raw.txt")
        def functionDefinitions = new File(outputDirectory, "functions_definition_raw.txt")
        def functionReferences = new File(outputDirectory, "functions_reference_raw.txt")
        def dbFile = new File(outputDirectory, "gitdetective.db")
        dbFile.delete()
        def db = DBMaker
                .fileDB(dbFile)
                .fileMmapEnable()
                .make()

        vertx.executeBlocking({ future ->
            def buildDirectory = new File(job.data.getString("build_target")).parentFile.absolutePath
            def aliasMap = db.hashMap("aliasMap", Serializer.STRING, Serializer.STRING).create()
            def sourceLocationMap = db.hashMap("sourceLocationMap", Serializer.STRING, Serializer.INT_ARRAY).create()
            def qualifiedNameMap = db.hashMap("qualifiedNameMap", Serializer.STRING, Serializer.STRING).create()
            def functionNameSet = db.hashSet("functionNameSet", Serializer.STRING).create()

            filesOutput.append("fileLocation|filename|qualifiedName\n")
            functionDefinitions.append("file|name|qualifiedName|startOffset|endOffset\n")
            functionReferences.append("xFunction|yFunction|qualifiedName|startOffset|endOffset|isExternal|isJdk\n")
            preprocessEntities(job, importFile, buildDirectory, qualifiedNameMap)
            processEntities(job, importFile, buildDirectory, aliasMap, sourceLocationMap, qualifiedNameMap,
                    functionNameSet, filesOutput)
            processRelationships(job, importFile, buildDirectory, aliasMap, sourceLocationMap, qualifiedNameMap,
                    functionNameSet, functionDefinitions, functionReferences)
            future.complete()
        }, false, { res ->
            db.close()

            if (res.failed()) {
                res.cause().printStackTrace()
                logPrintln(job, res.cause().getMessage())
                job.done(res.cause())
            } else {
                job.data.put("index_results_db", dbFile.absolutePath)
                job.save().setHandler({
                    vertx.eventBus().send(GitDetectiveImportFilter.GITDETECTIVE_IMPORT_FILTER, it.result())
                })
            }
        })
    }

    private static void preprocessEntities(Job job, File importFile, String buildDirectory,
                                           Map<String, String> qualifiedNameMap) {
        logPrintln(job, "Pre-processing entities")
        importFile.eachLine {
            String[] row = it.split(" ")
            String subject = toKytheGithubPath(buildDirectory, row[0].substring(1, row[0].length() - 1))

            String predicate = row[1].substring(1, row[1].length() - 1)
            if (predicate == "/kythe/edge/named") {
                String object = toKytheGithubPath(buildDirectory, it.substring(it.indexOf(
                        predicate) + predicate.length() + 3, it.length() - 3))

                qualifiedNameMap.put(subject, URLDecoder.decode(object, "UTF-8"))
            }
        }
    }

    private static void processEntities(Job job, File importFile, String buildDirectory,
                                        Map<String, String> aliasMap, Map<String, int[]> sourceLocationMap,
                                        Map<String, String> qualifiedNameMap, Set<String> functionNameSet,
                                        File filesOutput) {
        logPrintln(job, "Processing entities")
        importFile.eachLine {
            String[] row = it.split(" ")
            String fullSubjectPath = row[0].substring(1, row[0].length() - 1)
            String subject = toKytheGithubPath(buildDirectory, fullSubjectPath)

            String predicate = row[1].substring(1, row[1].length() - 1)
            if (KYTHE_PARSE_SET.contains(predicate)) {
                String object = toKytheGithubPath(buildDirectory, it.substring(it.indexOf(
                        predicate) + predicate.length() + 3, it.length() - 3))

                processRecordEntity(fullSubjectPath, subject, predicate, object, aliasMap, sourceLocationMap,
                        qualifiedNameMap, functionNameSet, filesOutput)
            }
        }
    }

    private static void processRelationships(Job job, File importFile, String buildDirectory,
                                             Map<String, String> aliasMap, Map<String, int[]> sourceLocationMap,
                                             Map<String, String> qualifiedNameMap, Set<String> functionNameSet,
                                             File definesOutput, File refcallsOutput) {
        logPrintln(job, "Processing relationships")
        importFile.eachLine {
            String[] row = it.split(" ")
            String fullSubjectPath = row[0].substring(1, row[0].length() - 1)
            String subject = toKytheGithubPath(buildDirectory, fullSubjectPath)

            String predicate = row[1].substring(1, row[1].length() - 1)
            if (KYTHE_PARSE_SET.contains(predicate)) {
                String object = toKytheGithubPath(buildDirectory, it.substring(it.indexOf(
                        predicate) + predicate.length() + 3, it.length() - 3))

                processRecordRelationship(subject, predicate, object, aliasMap, sourceLocationMap,
                        qualifiedNameMap, functionNameSet, definesOutput, refcallsOutput)
            }
        }
    }

    private static void processRecordEntity(String fullSubjectPath, String subject, String predicate, String object,
                                            Map<String, String> aliasMap, Map<String, int[]> sourceLocationMap,
                                            Map<String, String> qualifiedNameMap, Set<String> functionNameSet,
                                            File filesOutput) {
        if (predicate == "/kythe/node/kind") {
            if (object == "file" || object == "function") {
                if (!isJDK(subject)) {
                    if (object == "file") {
                        String fileLocation = fullSubjectPath.substring(fullSubjectPath.indexOf("path=") + 5)
                        filesOutput.append("$fileLocation|$subject|" + qualifiedNameMap.get(subject) + "\n")
                    }
                    if (object == "function") {
                        functionNameSet.add(subject)
                    }
                    aliasMap.put(subject, subject)
                }
            }
        } else if (predicate == "/kythe/edge/childof") {
            if (functionNameSet.contains(object)) {
                aliasMap.put(subject, object)
            } else {
                aliasMap.putIfAbsent(subject, object)
            }
        } else if (predicate == "/kythe/loc/start") {
            if (sourceLocationMap.containsKey(subject)) {
                sourceLocationMap.put(subject, [Integer.parseInt(object), sourceLocationMap.get(subject)[1]] as int[])
            } else {
                sourceLocationMap.put(subject, [Integer.parseInt(object), -1] as int[])
            }
        } else if (predicate == "/kythe/loc/end") {
            if (sourceLocationMap.containsKey(subject)) {
                sourceLocationMap.put(subject, [sourceLocationMap.get(subject)[0], Integer.parseInt(object)] as int[])
            } else {
                sourceLocationMap.put(subject, [-1, Integer.parseInt(object)] as int[])
            }
        }
    }

    private static void processRecordRelationship(String subject, String predicate, String object,
                                                  Map<String, String> aliasMap, Map<String, int[]> sourceLocationMap,
                                                  Map<String, String> qualifiedNameMap, Set<String> functionNameSet,
                                                  File functionDefinitions, File functionReferences) {
        if (!sourceLocationMap.containsKey(subject)) {
            return
        }
        def location = sourceLocationMap.get(subject)

        if (predicate == "/kythe/edge/ref/call") {
            if (!functionNameSet.contains(subject)) {
                subject = aliasMap.getOrDefault(subject, subject)
                while (subject != aliasMap.getOrDefault(subject, subject) && !functionNameSet.contains(subject)) {
                    subject = aliasMap.getOrDefault(subject, subject)
                }
            }
            if (!functionNameSet.contains(object)) {
                object = aliasMap.getOrDefault(object, object)
                while (object != aliasMap.getOrDefault(object, object) && !functionNameSet.contains(object)) {
                    object = aliasMap.getOrDefault(object, object)
                }
            }

            def qualifiedName = qualifiedNameMap.get(object)
            if (qualifiedName == null || !qualifiedName.endsWith(")")) {
                return //todo: understand why these exists and how to process
            } else if (isJDK(subject) || isJDK(object)) {
                return //no jdk
            }
            functionReferences.append("$subject|$object|$qualifiedName|" + location[0] + "|" + location[1] + "\n")
        } else if (predicate == "/kythe/edge/defines") {
            subject = aliasMap.getOrDefault(subject, subject)
            while (subject != aliasMap.getOrDefault(subject, subject)) {
                subject = aliasMap.getOrDefault(subject, subject)
            }
            object = aliasMap.getOrDefault(object, object)
            while (object != aliasMap.getOrDefault(object, object)) {
                object = aliasMap.getOrDefault(object, object)
            }

            def qualifiedName = qualifiedNameMap.get(object)
            if (qualifiedName == null || !qualifiedName.endsWith(")")) {
                return //todo: understand why these exists and how to process
            } else if (isJDK(subject) || isJDK(object)) {
                return //no jdk
            }
            functionDefinitions.append("$subject|$object|$qualifiedName|" + location[0] + "|" + location[1] + "\n")
        }
    }

    private static String toKytheGithubPath(String buildDirectory, String fullPath) {
        if (!fullPath.startsWith("kythe:") || !fullPath.contains("#")) {
            return fullPath
        }
        if (fullPath.startsWith("kythe:") && !fullPath.startsWith("kythe://jdk")) {
            fullPath = "kythe://github" + fullPath.substring(fullPath.indexOf("?"))
        }
        if (fullPath.contains("/src/main/java/")) {
            //todo: other languages + just redo this whole method :/
            fullPath = fullPath.substring(0, fullPath.indexOf("=") + 1) + "java?" +
                    fullPath.substring(fullPath.indexOf("/src/main/java/") + 15)
        }

        fullPath = fullPath.replace("path=%21jar%21/", "")
        fullPath = fullPath.replace("path=src/main/java/", "")
        fullPath = fullPath.replace("path=$buildDirectory/src/main/java/", "")
        fullPath = fullPath.replace("path=$buildDirectory/src/", "")
        fullPath = fullPath.replace(".class#", ".java#")
        if (fullPath.contains("/src/")) {
            def lang = "java" //todo: other languages
            fullPath = "kythe://github?lang=$lang?" + fullPath.substring(fullPath.indexOf("/src/") + 5)
        }
        if (!fullPath.startsWith("kythe://github?lang=")) {
            fullPath = fullPath.replace("kythe://github?", "kythe://github?lang=java?") //todo: other languages
        }
        //todo: other languages!
        return fullPath.replace("kythe://github?lang=java#", "kythe://github?lang=java?")
    }

    static boolean isJDK(String kythePath) {
        return kythePath.startsWith("kythe://jdk") ||
                kythePath.startsWith("kythe://github?lang=java?java/") ||
                kythePath.startsWith("kythe://github?lang=java?javax/") ||
                kythePath.startsWith("kythe://github?lang=java?sun/")
    }

}
