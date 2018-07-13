package io.gitdetective.web.work.importer

import ai.grakn.*
import ai.grakn.engine.GraknConfig
import ai.grakn.graql.Query
import ai.grakn.graql.internal.query.QueryAnswer
import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSch
import com.jcraft.jsch.JSchException
import com.jcraft.jsch.Session
import io.gitdetective.web.WebLauncher
import io.gitdetective.web.dao.GraknDAO
import io.gitdetective.web.dao.RedisDAO
import io.gitdetective.web.work.calculator.GraknCalculator
import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.*
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import javassist.ClassPool
import javassist.CtClass
import javassist.CtMethod
import org.apache.commons.lang.SystemUtils
import org.joor.Reflect

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.zip.ZipFile

import static io.gitdetective.web.Utils.asPrettyTime
import static io.gitdetective.web.Utils.logPrintln

/**
 * Import augmented and filtered/funnelled Kythe compilation data into Grakn
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GraknImporter extends AbstractVerticle {

    public static final String GRAKN_INDEX_IMPORT_JOB_TYPE = "ImportGithubProject"
    private final static Logger log = LoggerFactory.getLogger(GraknImporter.class)
    private final static String GET_FILE = Resources.toString(Resources.getResource(
            "queries/import/get_file.gql"), Charsets.UTF_8)
    private final static String IMPORT_FILES = Resources.toString(Resources.getResource(
            "queries/import/import_files.gql"), Charsets.UTF_8)
    private final static String GET_DEFINITION_BY_FILE_NAME = Resources.toString(Resources.getResource(
            "queries/import/get_definition_by_file_name.gql"), Charsets.UTF_8)
    private final static String GET_INTERNAL_REFERENCE = Resources.toString(Resources.getResource(
            "queries/import/get_internal_reference.gql"), Charsets.UTF_8)
    private final static String GET_INTERNAL_REFERENCE_BY_FUNCTION_NAME = Resources.toString(Resources.getResource(
            "queries/import/get_internal_reference_by_function_name.gql"), Charsets.UTF_8)
    private final static String GET_INTERNAL_REFERENCE_BY_FILE_NAME = Resources.toString(Resources.getResource(
            "queries/import/get_internal_reference_by_file_name.gql"), Charsets.UTF_8)
    private final static String GET_EXTERNAL_REFERENCE_BY_FILE_NAME = Resources.toString(Resources.getResource(
            "queries/import/get_external_reference_by_file_name.gql"), Charsets.UTF_8)
    private final static String GET_EXTERNAL_REFERENCE = Resources.toString(Resources.getResource(
            "queries/import/get_external_reference.gql"), Charsets.UTF_8)
    private final static String IMPORT_DEFINED_FUNCTIONS = Resources.toString(Resources.getResource(
            "queries/import/import_defined_functions.gql"), Charsets.UTF_8)
    private final static String IMPORT_INTERNAL_REFERENCED_FUNCTIONS = Resources.toString(Resources.getResource(
            "queries/import/import_internal_referenced_functions.gql"), Charsets.UTF_8)
    private final static String IMPORT_EXTERNAL_REFERENCED_FUNCTIONS = Resources.toString(Resources.getResource(
            "queries/import/import_external_referenced_functions.gql"), Charsets.UTF_8)
    private final static String IMPORT_EXTERNAL_REFERENCED_FUNCTION_BY_FILE = Resources.toString(Resources.getResource(
            "queries/import/import_external_referenced_function_by_file.gql"), Charsets.UTF_8)
    private final static String IMPORT_FILE_TO_FUNCTION_REFERENCE = Resources.toString(Resources.getResource(
            "queries/import/import_file_to_function_reference.gql"), Charsets.UTF_8)
    private final Kue kue
    private final RedisDAO redis
    private final GraknDAO grakn
    private final String uploadsDirectory
    private GraknSession graknSession
    private Session remoteSFTPSession

    GraknImporter(Kue kue, RedisDAO redis, GraknDAO grakn, String uploadsDirectory) {
        this.kue = kue
        this.redis = redis
        this.grakn = grakn
        this.uploadsDirectory = uploadsDirectory
    }

    @Override
    void start() throws Exception {
        String uploadsHost = config().getString("uploads.host")
        if (uploadsHost != null) {
            log.info "Connecting to remote SFTP server"
            String uploadsUsername = config().getString("uploads.username")
            String uploadsPassword = config().getString("uploads.password")
            remoteSFTPSession = new JSch().getSession(uploadsUsername, uploadsHost, 22)
            remoteSFTPSession.setConfig("StrictHostKeyChecking", "no")
            remoteSFTPSession.setPassword(uploadsPassword)
            remoteSFTPSession.connect()
            log.info "Connected to remote SFTP server"
        }

        String graknHost = config().getString("grakn.host")
        int graknPort = config().getInteger("grakn.port")
        String graknKeyspace = config().getString("grakn.keyspace")
        def keyspace = Keyspace.of(graknKeyspace)
        graknSession = Grakn.session(graknHost + ":" + graknPort, keyspace)
        if (SystemUtils.IS_OS_WINDOWS) {
            //start of hacks because Grakn doesn't make things easy for Windows :/
            try {
                GraknConfig config = Reflect.on(graknSession).get("config")
                config.setConfigProperty(GraknConfigKey.STORAGE_HOSTNAME, "192.168.99.100")

                CtClass clazz = ClassPool.getDefault().get("org.apache.cassandra.thrift.EndpointDetails")
                CtMethod originalMethod = clazz.getDeclaredMethod("getHost")
                originalMethod.setBody("return \"" + graknHost + "\";")
                clazz.toClass()
            } catch (Exception e) {
                e.printStackTrace()
            }
            //end of hacks because Grakn didn't make things easy for Windows :/
        }

        def importerConfig = config().getJsonObject("importer")
        def graknImportMeter = WebLauncher.metrics.meter("GraknImportJobProcessSpeed")
        kue.on("error", {
            log.error "Import job error: " + it.body()
        })
        kue.process(GRAKN_INDEX_IMPORT_JOB_TYPE, importerConfig.getInteger("thread_count"), { parentJob ->
            graknImportMeter.mark()
            log.info "Import job rate: " + (graknImportMeter.oneMinuteRate * 60) +
                    " per/min - Thread: " + Thread.currentThread().name

            def indexJob = parentJob
            indexJob.removeOnComplete = true
            def job = new Job(parentJob.data)
            vertx.executeBlocking({
                processImportJob(job, indexJob, it.completer())
            }, false, { result ->
                if (result.failed()) {
                    job.done(result.cause())
                    job.failed().setHandler({
                        indexJob.done(result.cause())
                    })
                } else {
                    job.done()
                    job.complete().setHandler({
                        indexJob.done()
                    })
                }
            })
        })
        log.info "GraknImporter started"
    }

    @Override
    void stop() throws Exception {
        remoteSFTPSession?.disconnect()
        graknSession?.close()
    }

    private void processImportJob(Job job, Job indexJob, Handler<AsyncResult> handler) {
        String githubRepo = job.data.getString("github_repository").toLowerCase()
        log.info "Importing project: " + githubRepo
        try {
            def outputDirectory = downloadAndExtractImportFiles(job)
            importProject(outputDirectory, job, {
                log.info "Finished importing project: " + githubRepo
                logPrintln(job, "Index results imported")

                def jobData = indexJob.data
                jobData.put("type", GraknCalculator.GRAKN_CALCULATE_JOB_TYPE)
                kue.createJob(GraknCalculator.GRAKN_CALCULATE_JOB_TYPE, jobData)
                        .setMax_attempts(0)
                        .setRemoveOnComplete(true)
                        .setPriority(job.priority)
                        .save().setHandler({
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        logPrintln(job, "Calculator received job")
                        handler.handle(Future.succeededFuture())
                    }
                })
            })
        } catch (ImportTimeoutException e) {
            logPrintln(job, "Project import timed out")
            handler.handle(Future.failedFuture(e))
        } catch (all) {
            all.printStackTrace()
            logPrintln(job, "Project failed to import")
            handler.handle(Future.failedFuture(all))
        }
    }

    private void importProject(File outputDirectory, Job job, Handler<AsyncResult> handler) {
        def timeoutTime = Instant.now().plus(1, ChronoUnit.HOURS)
        def filesOutput = new File(outputDirectory, "files.txt")
        def osFunctionsOutput = new File(outputDirectory, "functions_open-source.txt")
        def functionDefinitions = new File(outputDirectory, "functions_definition.txt")
        def functionReferences = new File(outputDirectory, "functions_reference.txt")
        def githubRepo = job.data.getString("github_repository").toLowerCase()
        def cacheFutures = new ArrayList<Future>()
        def importData = new ImportSessionData()
        boolean newProject = false
        String projectId = null

        def tx = graknSession.open(GraknTxType.WRITE)
        try {
            def graql = tx.graql()
            def res = graql.parse(GraknDAO.GET_PROJECT
                    .replace("<githubRepo>", githubRepo)).execute() as List<QueryAnswer>
            if (!res.isEmpty()) {
                logPrintln(job, "Updating existing project")
                projectId = res.get(0).get("p").asEntity().id.toString()
            } else {
                newProject = true
                logPrintln(job, "Creating new project")
                def query = graql.parse(GraknDAO.CREATE_PROJECT
                        .replace("<githubRepo>", githubRepo)
                        .replace("<createDate>", Instant.now().toString()))
                projectId = (query.execute() as List<QueryAnswer>).get(0).get("p").asEntity().id.toString()
                WebLauncher.metrics.counter("CreateProject").inc()
            }
            tx.commit()
        } finally {
            tx.close()
        }

        //open source functions
        importOpenSourceFunctions(job, osFunctionsOutput, timeoutTime, importData, cacheFutures)
        //project files
        long fileCount = importFiles(projectId, job, filesOutput, timeoutTime, newProject, importData, cacheFutures)
        //project definitions
        importDefinitions(projectId, job, functionDefinitions, timeoutTime, newProject, importData, cacheFutures, {
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                long methodInstanceCount = it.result()

                //project references
                importReferences(projectId, job, functionReferences, timeoutTime, newProject, importData, cacheFutures, {
                    if (it.failed()) {
                        handler.handle(Future.failedFuture(it.cause()))
                    } else {
                        //cache new project/file counts; then finished
                        logPrintln(job, "Caching import data")
                        def cacheInfoTimer = WebLauncher.metrics.timer("CachingProjectInformation")
                        def cacheInfoContext = cacheInfoTimer.time()
                        def fut1 = Future.future()
                        cacheFutures.add(fut1)
                        redis.incrementCachedProjectFileCount(githubRepo, fileCount, fut1.completer())
                        def fut2 = Future.future()
                        cacheFutures.add(fut2)
                        redis.incrementCachedProjectMethodInstanceCount(githubRepo, methodInstanceCount, fut2.completer())
                        CompositeFuture.all(cacheFutures).setHandler({
                            if (it.failed()) {
                                handler.handle(Future.failedFuture(it.cause()))
                            } else {
                                logPrintln(job, "Caching import data took: " + asPrettyTime(cacheInfoContext.stop()))
                                handler.handle(Future.succeededFuture())
                            }
                        })
                    }
                })
            }
        })
    }

    private void importOpenSourceFunctions(Job job, File osFunctionsOutput, Instant timeoutTime,
                                           ImportSessionData importData, List<Future> cacheFutures) {
        def tx
        logPrintln(job, "Importing open source functions")
        def importOSFTimer = WebLauncher.metrics.timer("ImportingOSFunctions")
        def importOSFContext = importOSFTimer.time()
        tx = graknSession.open(GraknTxType.BATCH)
        try {
            def graql = tx.graql()
            def lineNumber = 0
            osFunctionsOutput.eachLine {
                if (Instant.now().isAfter(timeoutTime)) throw new ImportTimeoutException()
                lineNumber++
                if (lineNumber > 1) {
                    def lineData = it.split("\\|")

                    def fut = Future.future()
                    cacheFutures.add(fut)
                    def osFunc = grakn.getOrCreateOpenSourceFunction(lineData[0], graql, fut.completer())
                    importData.openSourceFunctions.put(lineData[0], osFunc)
                }
            }
            tx.commit()
        } finally {
            tx.close()
        }
        logPrintln(job, "Importing open source functions took: " + asPrettyTime(importOSFContext.stop()))
    }

    private long importFiles(String projectId, Job job, File filesOutput, Instant timeoutTime, boolean newProject,
                             ImportSessionData importData, List<Future> cacheFutures) {
        long fileCount = 0
        def githubRepo = job.data.getString("github_repository").toLowerCase()
        def tx

        logPrintln(job, "Importing files")
        def importFilesTimer = WebLauncher.metrics.timer("ImportingFiles")
        def importFilesContext = importFilesTimer.time()
        tx = graknSession.open(GraknTxType.BATCH)
        try {
            def graql = tx.graql()
            def lineNumber = 0
            filesOutput.eachLine {
                if (Instant.now().isAfter(timeoutTime)) throw new ImportTimeoutException()
                lineNumber++
                if (lineNumber > 1) {
                    def lineData = it.split("\\|")
                    def importFile = newProject

                    if (!newProject) {
                        //find existing file
                        def query = graql.parse(GET_FILE
                                .replace("<filename>", lineData[1])
                                .replace("<projectId>", projectId))
                        def match = query.execute() as List<QueryAnswer>
                        importFile = match.isEmpty()
                        if (!importFile) {
                            def existingFileId = match.get(0).get("x").asEntity().id.toString()
                            def fut = Future.future()
                            cacheFutures.add(fut)
                            redis.cacheProjectImportedFile(githubRepo, lineData[1], existingFileId, fut.completer())
                            importData.importedFiles.put(lineData[1], existingFileId)
                        }
                    }

                    if (importFile) {
                        def importedFile = graql.parse(IMPORT_FILES
                                .replace("<projectId>", projectId)
                                .replace("<createDate>", Instant.now().toString())
                                .replace("<fileLocation>", lineData[0])
                                .replace("<filename>", lineData[1])
                                .replace("<qualifiedName>", lineData[2])).execute() as List<QueryAnswer>
                        def importedFileId = importedFile.get(0).get("f").asEntity().id.toString()
                        importData.importedFiles.put(lineData[1], importedFileId)
                        WebLauncher.metrics.counter("ImportFile").inc()
                        fileCount++

                        //cache imported file
                        def fut = Future.future()
                        cacheFutures.add(fut)
                        redis.cacheProjectImportedFile(githubRepo, lineData[1], importedFileId, fut.completer())
                    }
                }
            }
            tx.commit()
        } finally {
            tx.close()
        }
        logPrintln(job, "Importing files took: " + asPrettyTime(importFilesContext.stop()))
        return fileCount
    }

    private void importDefinitions(String projectId, Job job, File functionDefinitions, Instant timeoutTime,
                                   boolean newProject, ImportSessionData importData, List<Future> cacheFutures,
                                   Handler<AsyncResult<Long>> handler) {
        long methodInstanceCount = 0
        def githubRepo = job.data.getString("github_repository").toLowerCase()
        def commitSha1 = job.data.getString("commit")
        def commitDate = job.data.getString("commit_date")
        def tx

        log.debug("Preparing insert queries")
        logPrintln(job, "Importing defined functions")
        def importDefinitionsTimer = WebLauncher.metrics.timer("ImportingDefinedFunctions")
        def importDefinitionsContext = importDefinitionsTimer.time()
        def importFutures = new ArrayList<Future>()
        tx = graknSession.open(GraknTxType.BATCH)
        try {
            def graql = tx.graql()
            def lineNumber = 0
            functionDefinitions.eachLine {
                if (Instant.now().isAfter(timeoutTime)) throw new ImportTimeoutException()
                lineNumber++
                if (lineNumber > 1) {
                    def lineData = it.split("\\|")
                    def importDefinition = newProject
                    def fileId = importData.importedFiles.get(lineData[0])

                    if (!newProject) {
                        //find existing defined function
                        def existingDef = graql.parse(GET_DEFINITION_BY_FILE_NAME
                                .replace("<functionName>", lineData[2])
                                .replace("<filename>", lineData[0])).execute() as List<QueryAnswer>
                        importDefinition = existingDef.isEmpty()
                        if (!importDefinition) {
                            def existingFileId = existingDef.get(0).get("x").asEntity().id.toString()
                            def existingFunctionInstanceId = existingDef.get(0).get("y").asEntity().id.toString()
                            def existingFunctionId = existingDef.get(0).get("z").asEntity().id.toString()
                            def fut1 = Future.future()
                            def fut2 = Future.future()
                            cacheFutures.addAll(fut1, fut2)
                            redis.cacheProjectImportedFunction(githubRepo, lineData[1], existingFunctionId, fut1.completer())
                            redis.cacheProjectImportedDefinition(existingFileId, existingFunctionId, fut2.completer())

                            importData.definedFunctions.put(lineData[1], existingFunctionId)
                            importData.definedFunctionInstances.put(existingFunctionId, existingFunctionInstanceId)
                        }
                    }

                    if (importDefinition) {
                        def startOffset = lineData[3]
                        def endOffset = lineData[4]
                        def osFunc = importData.openSourceFunctions.get(lineData[1])
                        if (osFunc == null) {
                            def fut = Future.future()
                            cacheFutures.add(fut)
                            osFunc = grakn.getOrCreateOpenSourceFunction(lineData[1], graql, fut.completer())
                            importData.openSourceFunctions.put(lineData[1], osFunc)
                        }
                        if (fileId == null) {
                            //println "todo: me3" //todo: me
                            return
                        }

                        def fut = Future.future()
                        importFutures.add(fut)
                        redis.incrementFunctionDefinitionCount(osFunc.functionId, {
                            if (it.failed()) {
                                fut.fail(it.cause())
                            } else {
                                def importCode = new ImportableSourceCode()
                                importCode.fileId = fileId
                                importCode.functionId = osFunc.functionId
                                importCode.functionName = lineData[1]
                                importCode.insertQuery = graql.parse(IMPORT_DEFINED_FUNCTIONS
                                        .replace("<instanceOffset>", it.result().toString())
                                        .replace("<xFileId>", fileId)
                                        .replace("<projectId>", projectId)
                                        .replace("<funcDefsId>", osFunc.functionDefinitionsId)
                                        .replace("<createDate>", Instant.now().toString())
                                        .replace("<qualifiedName>", lineData[2])
                                        .replace("<commitSha1>", commitSha1)
                                        .replace("<commitDate>", commitDate)
                                        .replace("<startOffset>", startOffset)
                                        .replace("<endOffset>", endOffset))
                                fut.complete(importCode)
                            }
                        })
                    }
                }
            }
            tx.commit()
        } finally {
            tx.close()
        }

        CompositeFuture.all(importFutures).setHandler({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                log.debug("Executing insert queries")
                tx = graknSession.open(GraknTxType.BATCH)
                try {
                    def futures = it.result().list() as List<ImportableSourceCode>
                    for (def importCode : futures) {
                        def result = importCode.insertQuery.execute() as List<QueryAnswer>
                        importCode.functionInstanceId = result.get(0).get("y").asEntity().id.toString()
                        importData.definedFunctions.put(importCode.functionName, importCode.functionId)
                        importData.definedFunctionInstances.put(importCode.functionId, importCode.functionInstanceId)
                        WebLauncher.metrics.counter("ImportDefinedFunction").inc()
                        methodInstanceCount++

                        //cache imported function/definition
                        def fut1 = Future.future()
                        def fut2 = Future.future()
                        cacheFutures.addAll(fut1, fut2)
                        redis.cacheProjectImportedFunction(githubRepo, importCode.functionName, importCode.functionId, fut1.completer())
                        redis.cacheProjectImportedDefinition(importCode.fileId, importCode.functionId, fut2.completer())
                    }
                    tx.commit()
                } finally {
                    tx.close()
                }
                logPrintln(job, "Importing defined functions took: " + asPrettyTime(importDefinitionsContext.stop()))
                handler.handle(Future.succeededFuture(methodInstanceCount))
            }
        })
    }

    private void importReferences(String projectId, Job job, File functionReferences, Instant timeoutTime,
                                  boolean newProject, ImportSessionData importData, List<Future> cacheFutures,
                                  Handler<AsyncResult> handler) {
        def githubRepo = job.data.getString("github_repository").toLowerCase()
        def commitSha1 = job.data.getString("commit")
        def commitDate = job.data.getString("commit_date")
        def tx

        logPrintln(job, "Importing function references")
        def importReferencesTimer = WebLauncher.metrics.timer("ImportingReferences")
        def importReferencesContext = importReferencesTimer.time()
        def importFutures = new ArrayList<Future<Query>>()
        tx = graknSession.open(GraknTxType.BATCH)
        try {
            def graql = tx.graql()
            def lineNumber = 0
            functionReferences.eachLine {
                if (Instant.now().isAfter(timeoutTime)) throw new ImportTimeoutException()
                lineNumber++
                if (lineNumber > 1) {
                    def lineData = it.split("\\|")
                    def importReference = newProject
                    def isFileReferencing = !lineData[0].contains("#")
                    def isExternal = Boolean.valueOf(lineData[5])
                    def osFunc = importData.openSourceFunctions.get(lineData[1])
                    if (osFunc == null) {
                        def fut = Future.future()
                        cacheFutures.add(fut)
                        osFunc = grakn.getOrCreateOpenSourceFunction(lineData[1], graql, fut.completer())
                        importData.openSourceFunctions.put(lineData[1], osFunc)
                    }

                    if (!newProject) {
                        //find existing reference
                        if (isFileReferencing) {
                            def refFile = lineData[0]
                            if (isExternal) {
                                def existingRef = graql.parse(GET_EXTERNAL_REFERENCE_BY_FILE_NAME
                                        .replace("<xFileName>", refFile)
                                        .replace("<yFuncRefsId>", osFunc.functionReferencesId)).execute() as List<QueryAnswer>
                                importReference = existingRef.isEmpty()
                                if (!importReference) {
                                    def fileId = existingRef.get(0).get("file").asEntity().id.toString()
                                    def functionId = existingRef.get(0).get("func").asEntity().id.toString()
                                    def fut = Future.future()
                                    cacheFutures.addAll(fut)
                                    redis.cacheProjectImportedReference(fileId, functionId, fut.completer())
                                }
                            } else {
                                def existingRef = graql.parse(GET_INTERNAL_REFERENCE_BY_FILE_NAME
                                        .replace("<xFileName>", refFile)
                                        .replace("<yFuncDefsId>", osFunc.functionDefinitionsId)).execute() as List<QueryAnswer>
                                importReference = existingRef.isEmpty()
                                if (!importReference) {
                                    def fileId = existingRef.get(0).get("file").asEntity().id.toString()
                                    def functionId = existingRef.get(0).get("func").asEntity().id.toString()
                                    def fut = Future.future()
                                    cacheFutures.addAll(fut)
                                    redis.cacheProjectImportedReference(fileId, functionId, fut.completer())
                                }
                            }
                        } else {
                            if (isExternal) {
                                def defOsFunc = importData.openSourceFunctions.get(lineData[0])
                                if (defOsFunc == null) {
                                    def fut = Future.future()
                                    cacheFutures.add(fut)
                                    defOsFunc = grakn.getOrCreateOpenSourceFunction(lineData[0], graql, fut.completer())
                                    importData.openSourceFunctions.put(lineData[0], defOsFunc)
                                }

                                def existingRef = graql.parse(GET_EXTERNAL_REFERENCE
                                        .replace("<projectId>", projectId)
                                        .replace("<funcDefsId>", defOsFunc.functionDefinitionsId)
                                        .replace("<funcRefsId>", osFunc.functionReferencesId)).execute() as List<QueryAnswer>
                                importReference = existingRef.isEmpty()
                                if (!importReference) {
                                    def function1Id = existingRef.get(0).get("yFunc").asEntity().id.toString()
                                    def function2Id = existingRef.get(0).get("func").asEntity().id.toString()
                                    def fut1 = Future.future()
                                    def fut2 = Future.future()
                                    cacheFutures.addAll(fut1, fut2)
                                    redis.cacheProjectImportedFunction(githubRepo, lineData[1], osFunc.functionId, fut1.completer())
                                    redis.cacheProjectImportedReference(function1Id, function2Id, fut2.completer())
                                }
                            } else {
                                def refFunctionId = importData.definedFunctions.get(lineData[0])
                                if (refFunctionId == null) {
                                    def existingRef = graql.parse(GET_INTERNAL_REFERENCE_BY_FUNCTION_NAME
                                            .replace("<funcName1>", lineData[0])
                                            .replace("<funcName2>", lineData[1])).execute() as List<QueryAnswer>
                                    importReference = existingRef.isEmpty()
                                    if (!importReference) {
                                        def existingFunc1Id = existingRef.get(0).get("func1").asEntity().id.toString()
                                        def existingFunc2Id = existingRef.get(0).get("func2").asEntity().id.toString()
                                        def fut = Future.future()
                                        cacheFutures.addAll(fut)
                                        redis.cacheProjectImportedReference(existingFunc1Id, existingFunc2Id, fut.completer())
                                    }
                                } else {
                                    def funcId = importData.definedFunctions.get(lineData[1])
                                    if (funcId == null || importData.definedFunctionInstances.get(refFunctionId) == null
                                            || importData.definedFunctionInstances.get(funcId) == null) {
                                        //println "todo: me4" //todo: me
                                        return
                                    }

                                    def existingRef = graql.parse(GET_INTERNAL_REFERENCE
                                            .replace("<xFunctionInstanceId>", importData.definedFunctionInstances.get(refFunctionId))
                                            .replace("<yFunctionInstanceId>", importData.definedFunctionInstances.get(funcId))).execute() as List<QueryAnswer>
                                    importReference = existingRef.isEmpty()
                                    if (!importReference) {
                                        def fut = Future.future()
                                        cacheFutures.addAll(fut)
                                        redis.cacheProjectImportedReference(refFunctionId, funcId, fut.completer())
                                    }
                                }
                            }
                        }
                    }

                    if (importReference) {
                        def startOffset = lineData[3]
                        def endOffset = lineData[4]
                        def refFunctionId = importData.definedFunctions.get(lineData[0])
                        def importCode = new ImportableSourceCode()
                        importCode.isFileReferencing = isFileReferencing
                        importCode.isExternalReference = isExternal

                        if (isFileReferencing) {
                            //reference from files
                            def fileId = importData.importedFiles.get(lineData[0])
                            if (fileId == null) {
                                //println "todo: me2" //todo: me
                                return
                            }

                            if (isExternal) {
                                //internal file references external function
                                def fut = Future.future()
                                importFutures.add(fut)
                                importCode.fileId = fileId
                                importCode.referenceFunctionId = osFunc.functionId
                                redis.incrementFunctionReferenceCount(importCode.referenceFunctionId, {
                                    if (it.failed()) {
                                        fut.fail(it.cause())
                                    } else {
                                        importCode.insertQuery = graql.parse(IMPORT_EXTERNAL_REFERENCED_FUNCTION_BY_FILE
                                                .replace("<instanceOffset>", it.result().toString())
                                                .replace("<xFileId>", importCode.fileId)
                                                .replace("<projectId>", projectId)
                                                .replace("<funcRefsId>", osFunc.functionReferencesId)
                                                .replace("<createDate>", Instant.now().toString())
                                                .replace("<qualifiedName>", lineData[2])
                                                .replace("<commitSha1>", commitSha1)
                                                .replace("<commitDate>", commitDate)
                                                .replace("<startOffset>", startOffset)
                                                .replace("<endOffset>", endOffset)
                                                .replace("<isJdk>", lineData[6]))
                                        fut.complete(importCode)
                                    }
                                })
                            } else {
                                //internal file references internal function
                                def funcId = importData.definedFunctions.get(lineData[1])
                                if (funcId == null || importData.definedFunctionInstances.get(funcId) == null) {
                                    //println "todo: me3" //todo: me
                                    return
                                }

                                def fut = Future.future()
                                importFutures.add(fut)
                                importCode.fileId = fileId
                                importCode.referenceFunctionId = funcId
                                importCode.referenceFunctionInstanceId = importData.definedFunctionInstances.get(funcId)
                                redis.incrementFunctionReferenceCount(importCode.referenceFunctionId, {
                                    if (it.failed()) {
                                        fut.fail(it.cause())
                                    } else {
                                        importCode.insertQuery = graql.parse(IMPORT_FILE_TO_FUNCTION_REFERENCE
                                                .replace("<xFileId>", importCode.fileId)
                                                .replace("<yFuncInstanceId>", importCode.referenceFunctionInstanceId)
                                                .replace("<createDate>", Instant.now().toString())
                                                .replace("<startOffset>", startOffset)
                                                .replace("<endOffset>", endOffset)
                                                .replace("<isJdk>", lineData[6]))
                                        fut.complete(importCode)
                                    }
                                })
                            }
                        } else {
                            //references from functions
                            if (isExternal) {
                                //internal function references external function
                                def fut = Future.future()
                                importFutures.add(fut)
                                importCode.functionId = refFunctionId
                                importCode.functionInstanceId = importData.definedFunctionInstances.get(refFunctionId)
                                importCode.referenceFunctionId = osFunc.functionId
                                redis.incrementFunctionReferenceCount(importCode.referenceFunctionId, {
                                    if (it.failed()) {
                                        fut.fail(it.cause())
                                    } else {
                                        importCode.insertQuery = graql.parse(IMPORT_EXTERNAL_REFERENCED_FUNCTIONS
                                                .replace("<instanceOffset>", it.result().toString())
                                                .replace("<xFuncInstanceId>", importCode.functionInstanceId)
                                                .replace("<projectId>", projectId)
                                                .replace("<funcRefsId>", osFunc.functionReferencesId)
                                                .replace("<createDate>", Instant.now().toString())
                                                .replace("<qualifiedName>", lineData[2])
                                                .replace("<commitSha1>", commitSha1)
                                                .replace("<commitDate>", commitDate)
                                                .replace("<startOffset>", startOffset)
                                                .replace("<endOffset>", endOffset)
                                                .replace("<isJdk>", lineData[6]))
                                        fut.complete(importCode)
                                    }
                                })
                            } else {
                                //internal function references internal function
                                def funcId = importData.definedFunctions.get(lineData[1])
                                if (funcId == null) {
                                    //println "todo: me" //todo: me
                                    return
                                }

                                def fut = Future.future()
                                importFutures.add(fut)
                                importCode.functionId = refFunctionId
                                importCode.functionInstanceId = importData.definedFunctionInstances.get(refFunctionId)
                                importCode.referenceFunctionId = funcId
                                redis.incrementFunctionReferenceCount(importCode.referenceFunctionId, {
                                    if (it.failed()) {
                                        fut.fail(it.cause())
                                    } else {
                                        importCode.referenceFunctionInstanceId = importData.definedFunctionInstances.get(funcId)
                                        importCode.insertQuery = graql.parse(IMPORT_INTERNAL_REFERENCED_FUNCTIONS
                                                .replace("<xFuncInstanceId>", importCode.functionInstanceId)
                                                .replace("<yFuncInstanceId>", importCode.referenceFunctionInstanceId)
                                                .replace("<createDate>", Instant.now().toString())
                                                .replace("<startOffset>", startOffset)
                                                .replace("<endOffset>", endOffset)
                                                .replace("<isJdk>", lineData[6]))
                                        fut.complete(importCode)
                                    }
                                })
                            }
                        }
                        WebLauncher.metrics.counter("ImportReferencedFunction").inc()
                    }
                }
            }
            tx.commit()
        } finally {
            tx.close()
        }

        CompositeFuture.all(importFutures).setHandler({
            if (it.failed()) {
                handler.handle(Future.failedFuture(it.cause()))
            } else {
                log.debug("Executing insert queries")
                tx = graknSession.open(GraknTxType.BATCH)
                try {
                    def futures = it.result().list() as List<ImportableSourceCode>
                    for (def importCode : futures) {
                        importCode.insertQuery.execute() as List<QueryAnswer>

                        if (importCode.isFileReferencing) {
                            if (importCode.isExternalReference) {
                                //cache imported function/reference (internal function -> external function)
                                //def fut1 = Future.future()
                                def fut2 = Future.future()
                                cacheFutures.addAll(fut2)
                                //redis.cacheProjectImportedFunction(githubRepo, importCode.referenceFunctionName, importCode.referenceFunctionId, fut1.completer())
                                redis.cacheProjectImportedReference(importCode.fileId, importCode.referenceFunctionId, fut2.completer())
                            } else {
                                //cache imported reference (internal file -> internal function)
                                def fut = Future.future()
                                cacheFutures.add(fut)
                                redis.cacheProjectImportedReference(importCode.fileId, importCode.referenceFunctionId, fut.completer())
                            }
                        } else {
                            if (importCode.isExternalReference) {
                                //cache imported function/reference (internal function -> external function)
                                //def fut1 = Future.future()
                                def fut2 = Future.future()
                                cacheFutures.addAll(fut2)
                                //redis.cacheProjectImportedFunction(githubRepo, importCode.referenceFunctionName, importCode.referenceFunctionId, fut1.completer())
                                redis.cacheProjectImportedReference(importCode.functionId, importCode.referenceFunctionId, fut2.completer())
                            } else {
                                //cache imported reference (internal function -> internal function)
                                def fut = Future.future()
                                cacheFutures.add(fut)
                                redis.cacheProjectImportedReference(importCode.functionId, importCode.referenceFunctionId, fut.completer())
                            }
                        }
                    }
                    tx.commit()
                } finally {
                    tx.close()
                }
                logPrintln(job, "Importing function references took: " + asPrettyTime(importReferencesContext.stop()))
                handler.handle(Future.succeededFuture())
            }
        })
    }

    private File downloadAndExtractImportFiles(Job job) {
        logPrintln(job, "Staging index results")
        def indexZipUuid = job.data.getString("import_index_file_id")
        def remoteIndexResultsZip = new File(uploadsDirectory, indexZipUuid + ".zip")
        def indexResultsZip = new File(uploadsDirectory, indexZipUuid + ".zip")
        if (config().getString("uploads.directory.local") != null) {
            indexResultsZip = new File(config().getString("uploads.directory.local"), indexZipUuid + ".zip")
        }
        indexResultsZip.parentFile.mkdirs()

        String uploadsHost = config().getString("uploads.host")
        if (uploadsHost != null) {
            log.debug "Downloading index results from SFTP"

            //try to connect 3 times (todo: use retryer?)
            boolean connected = false
            def sftpChannel = null
            for (int i = 0; i < 3; i++) {
                try {
                    def channel = remoteSFTPSession.openChannel("sftp")
                    channel.connect()
                    sftpChannel = (ChannelSftp) channel
                    connected = true
                } catch (JSchException ex) {
                    Thread.sleep(1000)
                }
                if (connected) break
            }
            if (connected) {
                sftpChannel.get(remoteIndexResultsZip.absolutePath, indexResultsZip.absolutePath)
                //todo: delete remote zip
                sftpChannel.exit()
            } else {
                throw new IOException("Unable to download index result from remote SFTP server")
            }
        }

        def outputDirectory = new File(job.data.getString("output_directory"))
        outputDirectory.deleteDir()
        outputDirectory.mkdirs()

        def zipFile = new ZipFile(indexResultsZip)
        zipFile.entries().each { zipEntry ->
            def path = Paths.get(outputDirectory.absolutePath + File.separatorChar + zipEntry.name)
            if (zipEntry.directory) {
                Files.createDirectories(path)
            } else {
                def parentDir = path.getParent()
                if (!Files.exists(parentDir)) {
                    Files.createDirectories(parentDir)
                }
                Files.copy(zipFile.getInputStream(zipEntry), path)
            }
        }
        return outputDirectory
    }

}
