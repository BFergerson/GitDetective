package io.gitdetective.indexer.stage.extract

import com.google.common.collect.Sets
import com.google.devtools.kythe.proto.MarkedSource
import com.google.devtools.kythe.util.KytheURI
import io.gitdetective.indexer.stage.KytheIndexAugment
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
    private static final Set<String> KYTHE_ENTITY_PARSE_SET = Sets.newHashSet(
            "/kythe/node/kind", "/kythe/subkind",
            "/kythe/loc/start", "/kythe/loc/end")
    private static final Set<String> KYTHE_RELATIONSHIP_PARSE_SET = Sets.newHashSet(
            "/kythe/edge/childof", "/kythe/edge/ref/call")
    private static final File javacExtractor = new File("opt/kythe-v0.0.28/extractors/javac_extractor.jar")

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
            sourceUsage.fileLocations = db.hashMap("fileLocations", Serializer.STRING, Serializer.STRING).create()
            sourceUsage.aliasMap = db.hashMap("aliasMap", Serializer.STRING, Serializer.STRING).create()
            sourceUsage.paramToTypeMap = db.hashMap("paramToType", Serializer.STRING, Serializer.STRING).create()
            sourceUsage.sourceLocationMap = db.hashMap("sourceLocationMap", Serializer.STRING, Serializer.INT_ARRAY).create()
            sourceUsage.functionNameSet = db.hashSet("functionNameSet", Serializer.STRING).create()
            sourceUsage.definedFiles = db.hashSet("definedFiles", Serializer.STRING).create()
            sourceUsage.indexDataLimits = config().getJsonObject("index_data_limits")

            filesOutput.append("fileLocation|filename|qualifiedName\n")
            functionDefinitions.append("file|name|qualifiedName|startOffset|endOffset\n")
            functionReferences.append("fileLocation|xFileOrFunction|xQualifiedName|yFunction|yQualifiedName|startOffset|endOffset|isExternal|isJdk\n")
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
                vertx.eventBus().send(KytheIndexAugment.KYTHE_INDEX_AUGMENT, job)
            }
        })
    }

    private static void preprocessEntities(Job job, ExtractedSourceCodeUsage sourceUsage) {
        logPrintln(job, "Pre-processing entities")
        sourceUsage.importFile.eachLine {
            String[] row = ((it =~ '\"(.+)\" \"(.+)\" \"(.+)\"')[0] as String[]).drop(1)
            def subjectUri = toGithubCorpus(KytheURI.parse(row[0]))
            sourceUsage.getExtractedNode(subjectUri).uri = subjectUri

            String predicate = row[1]
            if (predicate == "/kythe/node/kind" && row[2] == "file") {
                sourceUsage.definedFiles.add(sourceUsage.getQualifiedName(subjectUri))
            } else if (predicate == "/kythe/subkind" && row[2] == "class") {
                def fileLocation = subjectUri.path
                if (fileLocation.contains("#")) {
                    fileLocation = fileLocation.substring(0, fileLocation.indexOf("#"))
                }
                sourceUsage.fileLocations.put(subjectUri.toString(), fileLocation)
            } else if (predicate == "/kythe/edge/defines/binding") {
                def objectUri = toGithubCorpus(KytheURI.parse(row[2]))
                sourceUsage.getExtractedNode(objectUri).uri = objectUri
                sourceUsage.addBinding(subjectUri, objectUri)
            } else if (predicate == "/kythe/edge/childof") {
                def objectUri = toGithubCorpus(KytheURI.parse(row[2]))
                sourceUsage.getExtractedNode(objectUri).uri = objectUri
                def parentNode = sourceUsage.getExtractedNode(objectUri)
                sourceUsage.getExtractedNode(subjectUri).setParentNode(parentNode)
            } else if (predicate.startsWith("/kythe/edge/param.")) {
                def objectUri = toGithubCorpus(KytheURI.parse(row[2]))
                def extractedFunction = sourceUsage.getExtractedNode(subjectUri)
                extractedFunction.addParam(predicate.replace("/kythe/edge/param.", "") as int, objectUri)
            } else if (predicate == "/kythe/edge/named") {
                def object = row[2]
                def extractedFunction = sourceUsage.getExtractedNode(subjectUri)
                def className = object.substring(object.indexOf("#") + 1)
                extractedFunction.context = className.substring(0, className.lastIndexOf(".") + 1)
                extractedFunction.identifier = className.substring(className.lastIndexOf(".") + 1)
            } else if (predicate == "/kythe/code") {
                def markedSource = MarkedSource.parseFrom(row[2].decodeBase64())
                if (markedSource.childCount == 0) {
                    return //nothing to do
                }

                def type = ""
                def context = ""
                def identifier = ""
                def isParam = false
                def hasInitializer = false
                def isFunction = false
                for (int i = 0; i < markedSource.childCount; i++) {
                    def child = markedSource.getChild(i)
                    if (child.kind == MarkedSource.Kind.TYPE) {
                        type = getType(child)
                        isParam = true
                    } else if (child.kind == MarkedSource.Kind.CONTEXT) {
                        context = getContext(child)
                    } else if (child.kind == MarkedSource.Kind.IDENTIFIER) {
                        identifier = child.preText
                    } else if (child.kind == MarkedSource.Kind.INITIALIZER) {
                        hasInitializer = true
                    } else if (child.kind == MarkedSource.Kind.PARAMETER_LOOKUP_BY_PARAM) {
                        isFunction = true
                    }
                }
                if (hasInitializer) {
                    return //need function definitions not function calls
                } else if (!isFunction && isParam) {
                    sourceUsage.paramToTypeMap.put(subjectUri.toString(), type)
                } else {
                    sourceUsage.getExtractedNode(subjectUri).uri = subjectUri
                    sourceUsage.getExtractedNode(subjectUri).context = context
                    sourceUsage.getExtractedNode(subjectUri).identifier = identifier
                    sourceUsage.getExtractedNode(subjectUri).isFunction = isFunction
                }
            }
        }
    }

    private static void processEntities(Job job, ExtractedSourceCodeUsage sourceUsage, File filesOutput) {
        logPrintln(job, "Processing entities")
        sourceUsage.importFile.eachLine {
            String[] row = ((it =~ '\"(.+)\" \"(.+)\" \"(.+)\"')[0] as String[]).drop(1)
            String subject = row[0]
            String predicate = row[1]
            if (KYTHE_ENTITY_PARSE_SET.contains(predicate)) {
                def object = row[2]
                processRecordEntity(subject, predicate, object, sourceUsage, filesOutput)
            }
        }
    }

    private static void processRelationships(Job job, ExtractedSourceCodeUsage sourceUsage,
                                             File definesOutput, File refcallsOutput) {
        logPrintln(job, "Processing relationships")
        sourceUsage.importFile.eachLine {
            String[] row = ((it =~ '\"(.+)\" \"(.+)\" \"(.+)\"')[0] as String[]).drop(1)
            def subject = row[0]
            String predicate = row[1]
            if (KYTHE_RELATIONSHIP_PARSE_SET.contains(predicate)) {
                def object = row[2]
                processRecordRelationship(subject, predicate, object, sourceUsage, definesOutput, refcallsOutput)
            }
        }
    }

    private static void processRecordEntity(String subject, String predicate, String object,
                                            ExtractedSourceCodeUsage sourceUsage, File filesOutput) {
        if (predicate == "/kythe/node/kind" || predicate == "/kythe/subkind") {
            if (object == "class" || object == "function") {
                if (!isJDK(subject)) {
                    def subjectUri = toGithubCorpus(KytheURI.parse(subject))
                    if (object == "class") {
                        sourceUsage.getExtractedNode(subjectUri).isFile = true
                        def fileLimit = sourceUsage.indexDataLimits.getInteger("files")
                        if (fileLimit == -1 || sourceUsage.fileCount++ < fileLimit) {
                            def fileLocation = subject.substring(subject.indexOf("path=") + 5)
                            if (fileLocation.contains("#")) {
                                fileLocation = fileLocation.substring(0, fileLocation.indexOf("#"))
                            }
                            def qualifiedName = sourceUsage.getQualifiedName(subjectUri)
                            if (sourceUsage.definedFiles.contains(qualifiedName)) {
                                filesOutput.append("$fileLocation|$subjectUri|$qualifiedName\n")
                            }
                        }
                    }
                    if (object == "function") {
                        sourceUsage.getExtractedNode(subjectUri).isFunction = true
                        sourceUsage.functionNameSet.add(subjectUri.signature)
                    }
                    sourceUsage.getExtractedNode(subjectUri).uri = subjectUri
                }
            }
        } else if (predicate == "/kythe/loc/start") {
            def subjectUri = toGithubCorpus(KytheURI.parse(subject))
            subjectUri = sourceUsage.getBindedNode(subjectUri).uri

            if (sourceUsage.sourceLocationMap.containsKey(subjectUri.signature)) {
                sourceUsage.sourceLocationMap.put(subjectUri.signature,
                        [Integer.parseInt(object), sourceUsage.sourceLocationMap.get(subjectUri.signature)[1]] as int[])
            } else {
                sourceUsage.sourceLocationMap.put(subjectUri.signature, [Integer.parseInt(object), -1] as int[])
            }
        } else if (predicate == "/kythe/loc/end") {
            def subjectUri = toGithubCorpus(KytheURI.parse(subject))
            subjectUri = sourceUsage.getBindedNode(subjectUri).uri

            if (sourceUsage.sourceLocationMap.containsKey(subjectUri.signature)) {
                sourceUsage.sourceLocationMap.put(subjectUri.signature,
                        [sourceUsage.sourceLocationMap.get(subjectUri.signature)[0], Integer.parseInt(object)] as int[])
            } else {
                sourceUsage.sourceLocationMap.put(subjectUri.signature, [-1, Integer.parseInt(object)] as int[])
            }
        }
    }

    private static void processRecordRelationship(String subject, String predicate, String object,
                                                  ExtractedSourceCodeUsage sourceUsage,
                                                  File functionDefinitions, File functionReferences) {
        def subjectUriOriginal = toGithubCorpus(KytheURI.parse(subject))
        def objectUriOriginal = toGithubCorpus(KytheURI.parse(object))
        def subjectNode = sourceUsage.getParentNode(subjectUriOriginal)
        def objectNode = sourceUsage.getParentNode(objectUriOriginal)

        int[] location
        if (subjectNode?.uri == null || objectNode?.uri == null) {
            return
        } else if (sourceUsage.sourceLocationMap.containsKey(subjectUriOriginal.toString())) {
            location = sourceUsage.sourceLocationMap.get(subjectUriOriginal.toString()) //file
        } else if (sourceUsage.sourceLocationMap.containsKey(subjectUriOriginal.signature)) {
            location = sourceUsage.sourceLocationMap.get(subjectUriOriginal.signature) //function
        } else {
            location = [-1, -1] //no code location
        }

        if (predicate == "/kythe/edge/childof") {
            if (isJDK(subjectNode.uri) || isJDK(objectNode.uri)) {
                return //no jdk
            } else if (!objectNode.isFile || !subjectNode.isFunction) {
                return //todo: what are these?
            }

            def subjectUri = subjectNode.uri
            def objectUri = objectNode.uri
            def qualifiedName = subjectNode.getQualifiedName(sourceUsage)
            functionDefinitions.append("$objectUri|$subjectUri|$qualifiedName|" + location[0] + "|" + location[1] + "\n")
        } else if (predicate == "/kythe/edge/ref/call") {
            if (isJDK(subjectNode.uri) || isJDK(objectNode.uri)) {
                return //no jdk
            } else if ((!(subjectNode.isFile || subjectNode.isFunction)) || !objectNode.isFunction) {
                return //todo: what are these?
            }

            def subjectUri = subjectNode.uri
            def objectUri = objectNode.uri
            def subjectQualifiedName = subjectNode.getQualifiedName(sourceUsage)
            def objectQualifiedName = objectNode.getQualifiedName(sourceUsage)
            def fileLocation = sourceUsage.fileLocations.get(subjectUri.toString())
            if (fileLocation == null) {
                fileLocation = Objects.requireNonNull(sourceUsage.fileLocations.get(subjectNode.parentNode.uri.toString()))
            }
            functionReferences.append("$fileLocation|$subjectUri|$subjectQualifiedName|$objectUri|$objectQualifiedName|"
                    + location[0] + "|" + location[1] + "\n")
        }
    }

    private static KytheURI toGithubCorpus(KytheURI uri) {
        if (uri.corpus == "jdk") return uri
        def githubUri = new KytheURI(uri.signature, "github", uri.root, uri.path, uri.language)
        def indexerPath = javacExtractor.absolutePath
        if (githubUri.path.contains(indexerPath)) {
            githubUri = new KytheURI(githubUri.signature, githubUri.corpus, githubUri.root,
                    githubUri.path
                            .replaceAll(indexerPath + "!/", "")
                            .replaceAll(indexerPath + "%21/", ""),
                    githubUri.language)
        }
        if (githubUri.path.startsWith("src/main/")) {
            githubUri = new KytheURI(githubUri.signature, githubUri.corpus, githubUri.root,
                    githubUri.path.replaceAll("(src/main/[^/]+/)", ""), githubUri.language)
        }
        if (githubUri.path.contains(".jar!")) {
            githubUri = new KytheURI(githubUri.signature, githubUri.corpus, githubUri.root,
                    githubUri.path.substring(githubUri.path.indexOf(".jar!") + 6), githubUri.language)
        }
        return githubUri
    }

    static boolean isJDK(KytheURI uri) {
        return isJDK(uri.toString())
    }

    static boolean isJDK(String uri) {
        return uri.startsWith("kythe://jdk") ||
                uri.startsWith("kythe://kythe?lang=java?java/") ||
                uri.startsWith("kythe://kythe?lang=java?javax/") ||
                uri.startsWith("kythe://kythe?lang=java?sun/") ||
                uri.startsWith("kythe://kythe?lang=java?com/sun/") ||
                uri.startsWith("kythe://github?lang=java?java/") ||
                uri.startsWith("kythe://github?lang=java?javax/") ||
                uri.startsWith("kythe://github?lang=java?sun/") ||
                uri.startsWith("kythe://github?lang=java?com/sun/")
    }

    private static String getType(MarkedSource markedSource) {
        if (markedSource.kind != MarkedSource.Kind.TYPE) {
            throw new IllegalArgumentException("Marked source missing context")
        }

        def type = ""
        for (int i = 0; i < markedSource.childCount; i++) {
            def child = markedSource.getChild(i)
            if (child.kind == MarkedSource.Kind.IDENTIFIER) {
                type += child.preText
                if ((i + 1) < markedSource.childCount || markedSource.addFinalListToken) {
                    type += markedSource.postChildText
                }
            } else if (child.kind == MarkedSource.Kind.CONTEXT) {
                type += getContext(child)
            }
        }

        def typeChild = markedSource.getChild(0)
        if (typeChild.kind == MarkedSource.Kind.BOX && typeChild.childCount == 1) {
            typeChild = typeChild.getChild(0)
        }
        for (int i = 0; i < typeChild.childCount; i++) {
            def child = typeChild.getChild(i)
            if (child.kind == MarkedSource.Kind.IDENTIFIER) {
                type += child.preText
                if ((i + 1) < typeChild.childCount || typeChild.addFinalListToken) {
                    type += typeChild.postChildText
                }
            } else if (child.kind == MarkedSource.Kind.CONTEXT) {
                type += getContext(child)
            }
        }
        return type
    }

    private static String getContext(MarkedSource markedSource) {
        if (markedSource.kind != MarkedSource.Kind.CONTEXT) {
            throw new IllegalArgumentException("Marked source missing context")
        }

        def context = ""
        for (int i = 0; i < markedSource.childCount; i++) {
            def child = markedSource.getChild(i)
            if (child.kind == MarkedSource.Kind.IDENTIFIER) {
                context += child.preText
                if ((i + 1) < markedSource.childCount || markedSource.addFinalListToken) {
                    context += markedSource.postChildText
                }
            }

            for (int z = 0; z < child.childCount; z++) {
                def grandChild = child.getChild(z)
                context += grandChild.preText
                if ((z + 1) < child.childCount || child.addFinalListToken) {
                    context += child.postChildText
                }
            }
        }
        return context
    }

}
