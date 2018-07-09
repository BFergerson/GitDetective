package io.gitdetective.web.work.importer

import ai.grakn.*
import ai.grakn.engine.GraknConfig
import ai.grakn.graql.internal.query.QueryAnswer
import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.jcraft.jsch.Channel
import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSch
import com.jcraft.jsch.Session
import io.gitdetective.web.WebLauncher
import io.gitdetective.web.dao.GraknDAO
import io.gitdetective.web.dao.RedisDAO
import io.gitdetective.web.work.calculator.GraknCalculator
import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.*
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
    private final static String GET_EXISTING_FILE = Resources.toString(Resources.getResource(
            "queries/import/get_existing_file.gql"), Charsets.UTF_8)
    private final static String IMPORT_FILES = Resources.toString(Resources.getResource(
            "queries/import/import_files.gql"), Charsets.UTF_8)
    private final static String GET_EXISTING_DEFINITION_BY_FILE_NAME = Resources.toString(Resources.getResource(
            "queries/import/get_existing_definition_by_file_name.gql"), Charsets.UTF_8)
    private final static String GET_EXISTING_INTERNAL_REFERENCE = Resources.toString(Resources.getResource(
            "queries/import/get_existing_internal_reference.gql"), Charsets.UTF_8)
    private final static String GET_EXISTING_INTERNAL_REFERENCE_BY_FUNCTION_NAME = Resources.toString(Resources.getResource(
            "queries/import/get_existing_internal_reference_by_function_name.gql"), Charsets.UTF_8)
    private final static String GET_EXISTING_INTERNAL_REFERENCE_BY_FILE_NAME = Resources.toString(Resources.getResource(
            "queries/import/get_existing_internal_reference_by_file_name.gql"), Charsets.UTF_8)
    private final static String GET_EXISTING_EXTERNAL_REFERENCE_BY_FILE_NAME = Resources.toString(Resources.getResource(
            "queries/import/get_existing_external_reference_by_file_name.gql"), Charsets.UTF_8)
    private final static String GET_EXISTING_EXTERNAL_REFERENCE = Resources.toString(Resources.getResource(
            "queries/import/get_existing_external_reference.gql"), Charsets.UTF_8)
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

    GraknImporter(Kue kue, RedisDAO redis, GraknDAO grakn, String uploadsDirectory) {
        this.kue = kue
        this.redis = redis
        this.grakn = grakn
        this.uploadsDirectory = uploadsDirectory
    }

    @Override
    void start() throws Exception {
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
            System.err.println("Import job error: " + it.body())
        })
        kue.process(GRAKN_INDEX_IMPORT_JOB_TYPE, importerConfig.getInteger("thread_count"), { parentJob ->
            graknImportMeter.mark()
            println "Import job rate: " + (graknImportMeter.oneMinuteRate * 60) +
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
        println "GraknImporter started"
    }

    private void processImportJob(Job job, Job indexJob, Handler<AsyncResult> handler) {
        String githubRepo = job.data.getString("github_repository").toLowerCase()
        println "Importing project: " + githubRepo
        try {
            def outputDirectory = downloadAndExtractImportFiles(job)
            importProject(outputDirectory, job, {
                println "Finished importing project: " + githubRepo
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
        long fileCount = 0
        long methodInstanceCount = 0
        def timeoutTime = Instant.now().plus(1, ChronoUnit.HOURS)
        def filesOutput = new File(outputDirectory, "files.txt")
        def osFunctionsOutput = new File(outputDirectory, "functions_open-source.txt")
        def functionDefinitions = new File(outputDirectory, "functions_definition.txt")
        def functionReferences = new File(outputDirectory, "functions_reference.txt")
        def githubRepo = job.data.getString("github_repository").toLowerCase()
        def commitSha1 = job.data.getString("commit")
        def commitDate = job.data.getString("commit_date")
        def cacheFutures = new ArrayList<Future>()

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

        logPrintln(job, "Importing open source functions")
        def openSourceFunctions = new HashMap<String, OpenSourceFunction>()
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
                    def osFunc = grakn.updateOpenSourceFunction(lineData[0], graql,
                            lineData[1] as long, lineData[2] as long)
                    openSourceFunctions.put(lineData[0], osFunc)
                }
            }
        } finally {
            tx.commit()
        }
        logPrintln(job, "Importing open source functions took: " + asPrettyTime(importOSFContext.stop()))

        logPrintln(job, "Importing files")
        def importedFiles = new HashMap<String, String>()
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
                        def query = graql.parse(GET_EXISTING_FILE
                                .replace("<filename>", lineData[1])
                                .replace("<projectId>", projectId))
                        def match = query.execute() as List<QueryAnswer>
                        importFile = match.isEmpty()
                    }

                    if (importFile) {
                        def importedFile = graql.parse(IMPORT_FILES
                                .replace("<projectId>", projectId)
                                .replace("<createDate>", Instant.now().toString())
                                .replace("<fileLocation>", lineData[0])
                                .replace("<filename>", lineData[1])
                                .replace("<qualifiedName>", lineData[2])).execute() as List<QueryAnswer>
                        def importedFileId = importedFile.get(0).get("f").asEntity().id.toString()
                        importedFiles.put(lineData[1], importedFileId)
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

        logPrintln(job, "Importing defined functions")
        def definedFunctions = new HashMap<String, String>()
        def definedFunctionInstances = new HashMap<String, String>()
        def importDefinitionsTimer = WebLauncher.metrics.timer("ImportingDefinedFunctions")
        def importDefinitionsContext = importDefinitionsTimer.time()
        tx = graknSession.open(GraknTxType.BATCH)
        try {
            def graql = tx.graql()
            def lineNumber = 0
            functionDefinitions.eachLine {
                if (Instant.now().isAfter(timeoutTime)) throw new ImportTimeoutException()
                lineNumber++
                if (lineNumber > 1) {
                    def lineData = it.split("\\|")
                    def fileId = importedFiles.get(lineData[0])
                    def importDefinition = newProject

                    if (!newProject) {
                        //find existing defined function
                        def existingDef = graql.parse(GET_EXISTING_DEFINITION_BY_FILE_NAME
                                .replace("<functionName>", lineData[2])
                                .replace("<filename>", lineData[0])).execute() as List<QueryAnswer>
                        importDefinition = existingDef.isEmpty()
                    }

                    if (importDefinition) {
                        def startOffset = lineData[3]
                        def endOffset = lineData[4]
                        def osFunc = openSourceFunctions.get(lineData[1])
                        def result = graql.parse(IMPORT_DEFINED_FUNCTIONS
                                .replace("<xFileId>", fileId)
                                .replace("<projectId>", projectId)
                                .replace("<funcDefsId>", osFunc.functionDefinitionsId)
                                .replace("<createDate>", Instant.now().toString())
                                .replace("<qualifiedName>", lineData[2])
                                .replace("<commitSha1>", commitSha1)
                                .replace("<commitDate>", commitDate)
                                .replace("<startOffset>", startOffset)
                                .replace("<endOffset>", endOffset)).execute() as List<QueryAnswer>
                        def importedFunctionId = result.get(0).get("y").asEntity().id.toString()
                        definedFunctions.put(lineData[1], osFunc.functionId)
                        definedFunctionInstances.put(osFunc.functionId, importedFunctionId)
                        WebLauncher.metrics.counter("ImportDefinedFunction").inc()
                        methodInstanceCount++

                        //cache imported function/definition
                        def fut1 = Future.future()
                        def fut2 = Future.future()
                        cacheFutures.addAll(fut1, fut2)
                        redis.cacheProjectImportedFunction(githubRepo, lineData[1], osFunc.functionId, fut1.completer())
                        redis.cacheProjectImportedDefinition(githubRepo, fileId, osFunc.functionId, fut2.completer())
                    }
                }
            }
        } finally {
            tx.commit()
        }
        logPrintln(job, "Importing defined functions took: " + asPrettyTime(importDefinitionsContext.stop()))

        logPrintln(job, "Importing function references")
        def importReferencesTimer = WebLauncher.metrics.timer("ImportingReferences")
        def importReferencesContext = importReferencesTimer.time()
        tx = graknSession.open(GraknTxType.BATCH)
        try {
            def graql = tx.graql()
            def lineNumber = 0
            functionReferences.eachLine {
                if (Instant.now().isAfter(timeoutTime)) throw new ImportTimeoutException()
                lineNumber++
                if (lineNumber > 1) {
                    def lineData = it.split("\\|")
                    def isFileReferencing = !lineData[0].contains("#")
                    def isExternal = Boolean.valueOf(lineData[5])
                    def osFunc = openSourceFunctions.get(lineData[1])
                    def importReference = newProject

                    if (!newProject) {
                        //find existing reference
                        if (isFileReferencing) {
                            def refFile = lineData[0]
                            if (isExternal) {
                                def existingRef = graql.parse(GET_EXISTING_EXTERNAL_REFERENCE_BY_FILE_NAME
                                        .replace("<xFileName>", refFile)
                                        .replace("<yFuncRefsId>", osFunc.functionReferencesId)
                                        .replace("<startOffset>", lineData[3])
                                        .replace("<endOffset>", lineData[4])).execute() as List<QueryAnswer>
                                importReference = existingRef.isEmpty()
                            } else {
                                def existingRef = graql.parse(GET_EXISTING_INTERNAL_REFERENCE_BY_FILE_NAME
                                        .replace("<xFileName>", refFile)
                                        .replace("<yFuncDefsId>", osFunc.functionDefinitionsId)
                                        .replace("<startOffset>", lineData[3])
                                        .replace("<endOffset>", lineData[4])).execute() as List<QueryAnswer>
                                importReference = existingRef.isEmpty()
                            }
                        } else {
                            if (isExternal) {
                                def defOsFunc = openSourceFunctions.get(lineData[0])
                                def existingRef = graql.parse(GET_EXISTING_EXTERNAL_REFERENCE
                                        .replace("<projectId>", projectId)
                                        .replace("<funcDefsId>", defOsFunc.functionDefinitionsId)
                                        .replace("<funcRefsId>", osFunc.functionReferencesId)
                                        .replace("<startOffset>", lineData[3])
                                        .replace("<endOffset>", lineData[4])).execute() as List<QueryAnswer>
                                importReference = existingRef.isEmpty()
                            } else {
                                def refFunctionId = definedFunctions.get(lineData[0])
                                if (refFunctionId == null) {
                                    def existingRef = graql.parse(GET_EXISTING_INTERNAL_REFERENCE_BY_FUNCTION_NAME
                                            .replace("<funcName1>", lineData[0])
                                            .replace("<funcName2>", lineData[1])
                                            .replace("<startOffset>", lineData[3])
                                            .replace("<endOffset>", lineData[4])).execute() as List<QueryAnswer>
                                    importReference = existingRef.isEmpty()
                                } else {
                                    def funcId = definedFunctions.get(lineData[1])
                                    def existingRef = graql.parse(GET_EXISTING_INTERNAL_REFERENCE
                                            .replace("<xFunctionInstanceId>", definedFunctionInstances.get(refFunctionId))
                                            .replace("<yFunctionInstanceId>", definedFunctionInstances.get(funcId))).execute() as List<QueryAnswer>
                                    importReference = existingRef.isEmpty()
                                }
                            }
                        }
                    }

                    if (importReference) {
                        def startOffset = lineData[3]
                        def endOffset = lineData[4]
                        def refFunctionId = definedFunctions.get(lineData[0])

                        if (refFunctionId == null) {
                            if (isFileReferencing) {
                                //reference from file to function
                                def fileId = importedFiles.get(lineData[0])
                                if (isExternal) {
                                    //from file to undefined function
                                    graql.parse(IMPORT_EXTERNAL_REFERENCED_FUNCTION_BY_FILE
                                            .replace("<xFileId>", fileId)
                                            .replace("<projectId>", projectId)
                                            .replace("<funcRefsId>", osFunc.functionReferencesId)
                                            .replace("<createDate>", Instant.now().toString())
                                            .replace("<qualifiedName>", lineData[2])
                                            .replace("<commitSha1>", commitSha1)
                                            .replace("<commitDate>", commitDate)
                                            .replace("<startOffset>", startOffset)
                                            .replace("<endOffset>", endOffset)
                                            .replace("<isJdk>", lineData[6])).execute() as List<QueryAnswer>

                                    //cache imported function/reference
                                    def fut1 = Future.future()
                                    def fut2 = Future.future()
                                    cacheFutures.addAll(fut1, fut2)
                                    redis.cacheProjectImportedFunction(githubRepo, lineData[1], osFunc.functionId, fut1.completer())
                                    redis.cacheProjectImportedReference(githubRepo, fileId, osFunc.functionId, fut2.completer())
                                } else {
                                    def funcId = definedFunctions.get(lineData[1])
                                    graql.parse(IMPORT_FILE_TO_FUNCTION_REFERENCE
                                            .replace("<xFileId>", fileId)
                                            .replace("<yFuncInstanceId>", definedFunctionInstances.get(funcId))
                                            .replace("<createDate>", Instant.now().toString())
                                            .replace("<startOffset>", startOffset)
                                            .replace("<endOffset>", endOffset)
                                            .replace("<isJdk>", lineData[6])).execute() as List<QueryAnswer>

                                    //cache imported reference
                                    def fut = Future.future()
                                    cacheFutures.add(fut)
                                    redis.cacheProjectImportedReference(githubRepo, fileId, funcId, fut.completer())
                                }
                            } else {
                                //reference to function not defined this import
                                println "todo: me" //todo: me
                                System.exit(-332)
                            }
                        } else {
                            if (isExternal) {
                                graql.parse(IMPORT_EXTERNAL_REFERENCED_FUNCTIONS
                                        .replace("<xFuncInstanceId>", definedFunctionInstances.get(refFunctionId))
                                        .replace("<projectId>", projectId)
                                        .replace("<funcRefsId>", osFunc.functionReferencesId)
                                        .replace("<createDate>", Instant.now().toString())
                                        .replace("<qualifiedName>", lineData[2])
                                        .replace("<commitSha1>", commitSha1)
                                        .replace("<commitDate>", commitDate)
                                        .replace("<startOffset>", startOffset)
                                        .replace("<endOffset>", endOffset)
                                        .replace("<isJdk>", lineData[6])).execute() as List<QueryAnswer>

                                //cache imported function/reference
                                def fut1 = Future.future()
                                def fut2 = Future.future()
                                cacheFutures.addAll(fut1, fut2)
                                redis.cacheProjectImportedFunction(githubRepo, lineData[1], osFunc.functionId, fut1.completer())
                                redis.cacheProjectImportedReference(githubRepo, refFunctionId, osFunc.functionId, fut2.completer())
                            } else {
                                def funcId = definedFunctions.get(lineData[1])
                                graql.parse(IMPORT_INTERNAL_REFERENCED_FUNCTIONS
                                        .replace("<xFuncInstanceId>", definedFunctionInstances.get(refFunctionId))
                                        .replace("<yFuncInstanceId>", definedFunctionInstances.get(funcId))
                                        .replace("<createDate>", Instant.now().toString())
                                        .replace("<startOffset>", startOffset)
                                        .replace("<endOffset>", endOffset)
                                        .replace("<isJdk>", lineData[6])).execute() as List<QueryAnswer>

                                //cache imported reference
                                def fut = Future.future()
                                cacheFutures.add(fut)
                                redis.cacheProjectImportedReference(githubRepo, refFunctionId, funcId, fut.completer())
                            }
                        }
                        WebLauncher.metrics.counter("ImportReferencedFunction").inc()
                    }
                }
            }
        } finally {
            tx.commit()
        }
        logPrintln(job, "Importing function references took: " + asPrettyTime(importReferencesContext.stop()))

        //todo: finalize. confirm counts; update if off

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
            println "Downloading index results from SFTP"
            String uploadsUsername = config().getString("uploads.username")
            String uploadsPassword = config().getString("uploads.password")
            JSch jsch = new JSch()
            Session session = jsch.getSession(uploadsUsername, uploadsHost, 22)
            session.setConfig("StrictHostKeyChecking", "no")
            session.setPassword(uploadsPassword)
            session.connect()

            Channel channel = session.openChannel("sftp")
            channel.connect()
            ChannelSftp sftpChannel = (ChannelSftp) channel
            sftpChannel.get(remoteIndexResultsZip.absolutePath, indexResultsZip.absolutePath)
            //todo: delete remote zip
            sftpChannel.exit()
            session.disconnect()
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
