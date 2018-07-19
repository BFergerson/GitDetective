package io.gitdetective.indexer.stage.extract

import com.google.common.collect.Sets
import com.google.devtools.kythe.proto.MarkedSource
import com.google.protobuf.ByteString
import groovy.json.StringEscapeUtils
import io.gitdetective.indexer.stage.GitDetectiveImportFilter
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import org.mapdb.DBMaker
import org.mapdb.Serializer

import static io.gitdetective.indexer.IndexerServices.logPrintln

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
            def sourceUsage = new ExtractedSourceCodeUsage()
            sourceUsage.importFile = importFile
            sourceUsage.buildDirectory = new File(job.data.getString("build_target")).parentFile.absolutePath
            sourceUsage.aliasMap = db.hashMap("aliasMap", Serializer.STRING, Serializer.STRING).create()
            sourceUsage.sourceLocationMap = db.hashMap("sourceLocationMap", Serializer.STRING, Serializer.INT_ARRAY).create()
            sourceUsage.qualifiedNameMap = db.hashMap("qualifiedNameMap", Serializer.STRING, Serializer.STRING).create()
            sourceUsage.classToSourceMap = db.hashMap("classToSourceMap", Serializer.STRING, Serializer.STRING).create()
            sourceUsage.functionNameSet = db.hashSet("functionNameSet", Serializer.STRING).create()
            sourceUsage.indexDataLimits = config().getJsonObject("index_data_limits")

            filesOutput.append("fileLocation|filename|qualifiedName\n")
            functionDefinitions.append("file|name|qualifiedName|startOffset|endOffset\n")
            functionReferences.append("xFunction|yFunction|qualifiedName|startOffset|endOffset|isExternal|isJdk\n")
            preprocessEntities(job, sourceUsage)
            processEntities(job, sourceUsage, filesOutput)
            processRelationships(job, sourceUsage, functionDefinitions, functionReferences)
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

    private static void preprocessEntities(Job job, ExtractedSourceCodeUsage sourceUsage) {
        logPrintln(job, "Pre-processing entities")
        sourceUsage.importFile.eachLine {
            String[] row = ((it =~ '\"(.+)\" \"(.+)\" \"(.+)\"')[0] as String[]).drop(1)
            String subject = toKytheGithubPath(sourceUsage.buildDirectory, row[0])
            if (!subject.contains(".class#") && subject.contains("#")) {
                sourceUsage.classToSourceMap.put(subject.substring(subject.lastIndexOf("#")), subject)
            }

            String predicate = row[1]
            if (predicate == "/kythe/edge/named") {
                String object = toKytheGithubPath(sourceUsage.buildDirectory, row[2])
                if (!object.contains(".class#") && object.contains("#")) {
                    sourceUsage.classToSourceMap.put(object.substring(object.lastIndexOf("#")), object)
                }

                sourceUsage.qualifiedNameMap.put(subject, URLDecoder.decode(object, "UTF-8"))
            } else if (predicate == "/kythe/code") {
                try {
                    MarkedSource.parseFrom(ByteString.copyFromUtf8(StringEscapeUtils.unescapeJava(row[2])).toByteArray())
                } catch (all) {
                    log.error "Couldn't parse: " + row[2]
                    return
                    /*
MarkedSource.parseFrom(ByteString.copyFromUtf8(
row[2].replaceAll(/\\u(....)/) {
    return Integer.parseInt(it[1], 16) as char
}
)).toByteArray()
                     */
                }
                def markedSource = MarkedSource.parseFrom(ByteString.copyFromUtf8(
                        StringEscapeUtils.unescapeJava(row[2])).toByteArray())
                if (markedSource.childCount == 0) {
                    return //nothing to do
                }

                def hasInitializer = false
                def context = ""
                def identifier = ""
                def functionArgs = ""
                for (int i = 0; i < markedSource.childCount; i++) {
                    def child = markedSource.getChild(i)
                    if (child.kind == MarkedSource.Kind.CONTEXT) {
                        for (int z = 0; z < child.childCount; z++) {
                            def grandChild = child.getChild(z)
                            context += grandChild.preText

                            if ((z + 1) < child.childCount || child.addFinalListToken) {
                                context += child.postChildText
                            }
                        }
                    } else if (child.kind == MarkedSource.Kind.IDENTIFIER) {
                        identifier = child.preText
                    } else if (child.kind == MarkedSource.Kind.PARAMETER_LOOKUP_BY_PARAM) {
                        functionArgs += child.preText
                        for (int z = 0; z < child.childCount; z++) {
                            def grandChild = child.getChild(z)
                            functionArgs += grandChild.preText

                            if ((z + 1) < child.childCount || child.addFinalListToken) {
                                functionArgs += child.postChildText
                            }
                        }
                        functionArgs += child.postText
                    } else if (child.kind == MarkedSource.Kind.INITIALIZER) {
                        hasInitializer = true
                    }
                }
                if (hasInitializer) {
                    return //need function definitions not function calls
                }

                if (functionArgs.isEmpty()) {
                    //class
                    def qualifiedName = context + identifier
                    sourceUsage.qualifiedNameMap.put(subject, URLDecoder.decode(qualifiedName, "UTF-8"))
                } else {
                    //function
                    def qualifiedName = context + identifier + functionArgs
                    sourceUsage.qualifiedNameMap.put(subject, URLDecoder.decode(qualifiedName, "UTF-8"))
                }
            }
        }
    }

    private static void processEntities(Job job, ExtractedSourceCodeUsage sourceUsage, File filesOutput) {
        logPrintln(job, "Processing entities")
        sourceUsage.importFile.eachLine {
            String[] row = ((it =~ '\"(.+)\" \"(.+)\" \"(.+)\"')[0] as String[]).drop(1)
            String fullSubjectPath = row[0]
            String subject = toKytheGithubPath(sourceUsage.buildDirectory, fullSubjectPath)
            if (subject.contains("#") && sourceUsage.classToSourceMap.containsKey(
                    subject.substring(subject.lastIndexOf("#")))) {
                subject = sourceUsage.classToSourceMap.get(subject.substring(subject.lastIndexOf("#")))
            }

            String predicate = row[1]
            if (KYTHE_PARSE_SET.contains(predicate)) {
                String object = toKytheGithubPath(sourceUsage.buildDirectory, row[2])
                if (object.contains("#") && sourceUsage.classToSourceMap.containsKey(
                        object.substring(object.lastIndexOf("#")))) {
                    object = sourceUsage.classToSourceMap.get(object.substring(object.lastIndexOf("#")))
                }
                processRecordEntity(fullSubjectPath, subject, predicate, object, sourceUsage, filesOutput)
            }
        }
    }

    private static void processRelationships(Job job, ExtractedSourceCodeUsage sourceUsage,
                                             File definesOutput, File refcallsOutput) {
        logPrintln(job, "Processing relationships")
        sourceUsage.importFile.eachLine {
            String[] row = ((it =~ '\"(.+)\" \"(.+)\" \"(.+)\"')[0] as String[]).drop(1)
            String fullSubjectPath = row[0]
            String subject = toKytheGithubPath(sourceUsage.buildDirectory, fullSubjectPath)
            if (subject.contains("#") && sourceUsage.classToSourceMap.containsKey(
                    subject.substring(subject.lastIndexOf("#")))) {
                subject = sourceUsage.classToSourceMap.get(subject.substring(subject.lastIndexOf("#")))
            }

            String predicate = row[1]
            if (KYTHE_PARSE_SET.contains(predicate)) {
                String object = toKytheGithubPath(sourceUsage.buildDirectory, row[2])
                if (object.contains("#") && sourceUsage.classToSourceMap.containsKey(
                        object.substring(object.lastIndexOf("#")))) {
                    object = sourceUsage.classToSourceMap.get(object.substring(object.lastIndexOf("#")))
                }
                processRecordRelationship(subject, predicate, object, sourceUsage, definesOutput, refcallsOutput)
            }
        }
    }

    private static void processRecordEntity(String fullSubjectPath, String subject, String predicate, String object,
                                            ExtractedSourceCodeUsage sourceUsage, File filesOutput) {
        if (predicate == "/kythe/node/kind") {
            if (object == "file" || object == "function") {
                if (!isJDK(subject)) {
                    if (object == "file") {
                        def fileLimit = sourceUsage.indexDataLimits.getInteger("files")
                        if (fileLimit == -1 || sourceUsage.fileCount++ < fileLimit) {
                            String fileLocation = fullSubjectPath.substring(fullSubjectPath.indexOf("path=") + 5)
                            filesOutput.append("$fileLocation|$subject|" + sourceUsage.qualifiedNameMap.get(subject) + "\n")
                        }
                    }
                    if (object == "function") {
                        sourceUsage.functionNameSet.add(subject)
                    }
                    sourceUsage.aliasMap.put(subject, subject)
                }
            }
        } else if (predicate == "/kythe/edge/childof") {
            if (sourceUsage.functionNameSet.contains(object)) {
                sourceUsage.aliasMap.put(subject, object)
            } else {
                sourceUsage.aliasMap.putIfAbsent(subject, object)
            }
        } else if (predicate == "/kythe/loc/start") {
            if (sourceUsage.sourceLocationMap.containsKey(subject)) {
                sourceUsage.sourceLocationMap.put(subject,
                        [Integer.parseInt(object), sourceUsage.sourceLocationMap.get(subject)[1]] as int[])
            } else {
                sourceUsage.sourceLocationMap.put(subject, [Integer.parseInt(object), -1] as int[])
            }
        } else if (predicate == "/kythe/loc/end") {
            if (sourceUsage.sourceLocationMap.containsKey(subject)) {
                sourceUsage.sourceLocationMap.put(subject,
                        [sourceUsage.sourceLocationMap.get(subject)[0], Integer.parseInt(object)] as int[])
            } else {
                sourceUsage.sourceLocationMap.put(subject, [-1, Integer.parseInt(object)] as int[])
            }
        }
    }

    private static void processRecordRelationship(String subject, String predicate, String object,
                                                  ExtractedSourceCodeUsage sourceUsage,
                                                  File functionDefinitions, File functionReferences) {
        if (!sourceUsage.sourceLocationMap.containsKey(subject)) {
            return
        }
        def location = sourceUsage.sourceLocationMap.get(subject)

        if (predicate == "/kythe/edge/defines") {
            subject = sourceUsage.aliasMap.getOrDefault(subject, subject)
            while (subject != sourceUsage.aliasMap.getOrDefault(subject, subject)) {
                subject = sourceUsage.aliasMap.getOrDefault(subject, subject)
            }
            object = sourceUsage.aliasMap.getOrDefault(object, object)
            while (object != sourceUsage.aliasMap.getOrDefault(object, object)) {
                object = sourceUsage.aliasMap.getOrDefault(object, object)
            }

            def qualifiedName = sourceUsage.qualifiedNameMap.get(object)
            if (isJDK(subject) || isJDK(object)) {
                return //no jdk
            } else if (qualifiedName == null || !qualifiedName.endsWith(")")) {
                return //todo: understand why these exists and how to process
            }
            functionDefinitions.append("$subject|$object|$qualifiedName|" + location[0] + "|" + location[1] + "\n")
        } else if (predicate == "/kythe/edge/ref/call") {
            if (!sourceUsage.functionNameSet.contains(subject)) {
                subject = sourceUsage.aliasMap.getOrDefault(subject, subject)
                while (subject != sourceUsage.aliasMap.getOrDefault(subject, subject)
                        && !sourceUsage.functionNameSet.contains(subject)) {
                    subject = sourceUsage.aliasMap.getOrDefault(subject, subject)
                }
            }
            if (!sourceUsage.functionNameSet.contains(object)) {
                object = sourceUsage.aliasMap.getOrDefault(object, object)
                while (object != sourceUsage.aliasMap.getOrDefault(object, object)
                        && !sourceUsage.functionNameSet.contains(object)) {
                    object = sourceUsage.aliasMap.getOrDefault(object, object)
                }
            }

            def qualifiedName = sourceUsage.qualifiedNameMap.get(object)
            if (isJDK(subject) || isJDK(object)) {
                return //no jdk
            } else if (qualifiedName == null || !qualifiedName.endsWith(")")) {
                return //todo: understand why these exists and how to process
            }
            functionReferences.append("$subject|$object|$qualifiedName|" + location[0] + "|" + location[1] + "\n")
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
                kythePath.startsWith("kythe://github?lang=java?sun/") ||
                kythePath.startsWith("kythe://github?lang=java?com/sun/")
    }

}
