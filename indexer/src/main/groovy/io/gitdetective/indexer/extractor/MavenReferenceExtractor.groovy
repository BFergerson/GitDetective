package io.gitdetective.indexer.extractor

import com.codebrig.arthur.observe.structure.filter.FunctionFilter
import com.codebrig.arthur.observe.structure.filter.MultiFilter
import com.codebrig.phenomena.Phenomena
import com.codebrig.phenomena.code.CodeObserverVisitor
import com.codebrig.phenomena.code.ContextualNode
import com.codebrig.phenomenon.kythe.KytheIndexObserver
import com.codebrig.phenomenon.kythe.build.KytheIndexBuilder
import com.codebrig.phenomenon.kythe.observe.KytheRefCallObserver
import com.google.common.collect.Lists
import groovy.util.logging.Slf4j
import io.gitdetective.indexer.extractor.observer.CompilationUnitObserver
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.PubSecKeyOptions
import io.vertx.ext.auth.jwt.JWTAuth
import io.vertx.ext.auth.jwt.JWTAuthOptions
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.predicate.ResponsePredicate

import java.time.Instant
import java.util.stream.Collectors

import static io.gitdetective.indexer.IndexerServices.logPrintln

@Slf4j
class MavenReferenceExtractor extends AbstractVerticle {

    public static final String EXTRACTOR_ADDRESS = "MavenReferenceExtractor"

    @Override
    void start() throws Exception {
        def provider = JWTAuth.create(vertx, new JWTAuthOptions()
                .addPubSecKey(new PubSecKeyOptions()
                        .setAlgorithm("HS256")
                        .setPublicKey(config().getString("gitdetective_service.api_key"))
                        .setSymmetric(true)))
        def apiKey = provider.generateToken(new JsonObject())

        vertx.eventBus().consumer(EXTRACTOR_ADDRESS, { msg ->
            def job = (Job) msg.body()
            extractProjectReferences(job, vertx, config(), apiKey, {
                logPrintln(job, "Pretend we did job")
                job.done()
            })
        })
        log.info "MavenReferenceExtractor started"
    }

    static void extractProjectReferences(Job job, Vertx vertx, JsonObject config, String apiKey,
                                         Handler<AsyncResult<Void>> handler) {
        def outDir = new File(job.data.getString("output_directory"))
        def kytheObservers = new ArrayList<KytheIndexObserver>()
        def refCallObserver = new KytheRefCallObserver()
        kytheObservers.add(refCallObserver)
        new KytheIndexBuilder(outDir)
                .setKytheOutputDirectory(outDir)
                .setKytheDirectory(new File("opt/kythe-v0.0.28")) //todo: download opt
                .build(kytheObservers)

        def phenomena = new Phenomena()
        phenomena.scanPath = new ArrayList<>()
        phenomena.scanPath.add(outDir.absolutePath)
        def visitor = new CodeObserverVisitor()
        visitor.addObservers(kytheObservers)
        def compilationUnitObserver = new CompilationUnitObserver()
        visitor.addObserver(compilationUnitObserver)
        phenomena.setupVisitor(visitor)
        phenomena.connectToBabelfish()
        phenomena.processScanPath().collect(Collectors.toList())
        phenomena.close()

        //todo: real method call filter
        def functionFilter = new FunctionFilter()
        def methodCallFilter = MultiFilter.matchAny()
                .reject(functionFilter, compilationUnitObserver.filter)
        def githubRepository = job.data.getString("github_repository")
        def username = githubRepository.substring(0, githubRepository.indexOf("/"))
        def projectName = githubRepository.substring(githubRepository.indexOf("/") + 1)
        WebClient client = WebClient.create(vertx)
        String host = config.getString("gitdetective_service.host")
        int port = config.getInteger("gitdetective_service.port")
        client.post(port, host, "/api/users/" + username + "/projects/" + projectName)
                .expect(ResponsePredicate.SC_OK)
                .bearerTokenAuthentication(apiKey).send({
            if (it.succeeded()) {
                def projectId = it.result().bodyAsString()
                createFiles(client, host, port, apiKey, projectId, job.data.getString("output_directory"),
                        visitor.observedContextualNodes.stream()
                                .filter({ compilationUnitObserver.filter.evaluate(it) })
                                .collect(Collectors.toList()), {
                    if (it.succeeded()) {
                        def fileIds = it.result()
                        createFunctions(client, host, port, apiKey, fileIds,
                                visitor.observedContextualNodes.stream()
                                        .filter({ !compilationUnitObserver.filter.evaluate(it) })
                                        .collect(Collectors.toList()), {
                            if (it.succeeded()) {
                                def functionIds = it.result()
                                insertReferences(client, host, port, apiKey, projectId,
                                        job.data.getInstant("commit_date"), job.data.getString("commit"), functionIds,
                                        visitor.observedContextualNodes.stream()
                                                .filter({ methodCallFilter.evaluate(it) })
                                                .collect(Collectors.toList()), {
                                    if (it.succeeded()) {
                                        handler.handle(Future.succeededFuture())
                                    } else {
                                        handler.handle(Future.failedFuture(it.cause()))
                                    }
                                })
                            } else {
                                handler.handle(Future.failedFuture(it.cause()))
                            }
                        })
                    } else {
                        handler.handle(Future.failedFuture(it.cause()))
                    }
                })
            } else {
                handler.handle(Future.failedFuture(it.cause()))
            }
        })
    }

    static void createFiles(WebClient client, String host, int port, String apiKey, String projectId,
                            String outputDirectory, List<ContextualNode> sourceFiles,
                            Handler<AsyncResult<Map<File, String>>> handler) {
        def fullOutputDirectory = outputDirectory
        if (!fullOutputDirectory.endsWith("/")) {
            fullOutputDirectory += "/"
        }
        def result = new HashMap<File, String>()
        def futures = []
        Lists.partition(sourceFiles, 100).each { list ->
            def request = new JsonArray()
            list.each {
                def req = new JsonObject()
                req.put("project_id", projectId)
                req.put("file_location", it.sourceFile.absolutePath.replaceFirst(fullOutputDirectory, ""))
                req.put("qualified_name", it.name)
                request.add(req)
            }

            def future = Future.future()
            futures << future
            client.post(port, host, "/api/files")
                    .expect(ResponsePredicate.SC_OK)
                    .bearerTokenAuthentication(apiKey).sendJson(request, {
                if (it.succeeded()) {
                    def resultArr = it.result().bodyAsJsonArray()
                    for (int i = 0; i < resultArr.size(); i++) {
                        result.put(list.get(i).sourceFile, resultArr.getString(i))
                    }
                    future.complete()
                } else {
                    future.fail(it.cause())
                }
            })
        }
        CompositeFuture.all(futures).setHandler({
            if (it.succeeded()) {
                handler.handle(Future.succeededFuture(result))
            } else {
                handler.handle(Future.failedFuture(it.cause()))
            }
        })
    }

    static void createFunctions(WebClient client, String host, int port, String apiKey,
                                Map<File, String> sourceFiles, List<ContextualNode> references,
                                Handler<AsyncResult<Map<String, String>>> handler) {
        def functionIds = new HashMap<String, String>()
        def futures = []
        Lists.partition(references, 100).each { list ->
            def request = new JsonArray()
            list.each {
                def kytheUri = it.attributes.get("kytheUri") ?: it.attributes.get("calledUri")
                def qualifiedName = it.hasName() ? it.name : it.attributes.get("calledQualifiedName")
                def functionInformation = new JsonObject()
                if (it.hasName()) {
                    //function declaration
                    functionInformation.put("file_id", sourceFiles.get(it.sourceFile))
                } else {
                    //function call
                    functionInformation.putNull("file_id")
                }
                functionInformation.put("kythe_uri", kytheUri)
                functionInformation.put("qualified_name", qualifiedName)
                request.add(functionInformation)
            }

            def future = Future.future()
            futures << future
            client.post(port, host, "/api/functions")
                    .expect(ResponsePredicate.SC_OK)
                    .bearerTokenAuthentication(apiKey).sendJson(request, {
                if (it.succeeded()) {
                    def response = it.result().bodyAsJsonArray()
                    for (int i = 0; i < response.size(); i++) {
                        functionIds.put(request.getJsonObject(i).getString("kythe_uri"), response.getString(i))
                    }
                    future.complete()
                } else {
                    future.fail(it.cause())
                }
            })
        }
        CompositeFuture.all(futures).setHandler({
            if (it.succeeded()) {
                handler.handle(Future.succeededFuture(functionIds))
            } else {
                handler.handle(Future.failedFuture(it.cause()))
            }
        })
    }

    static void insertReferences(WebClient client, String host, int port, String apiKey, String projectId,
                                 Instant commitDate, String commitSha1,
                                 Map<String, String> functionIds, List<ContextualNode> references,
                                 Handler<AsyncResult<Void>> handler) {
        def futures = []
        Lists.partition(references, 100).each { list ->
            def request = new JsonArray()
            list.each {
                def req = new JsonObject()
                req.put("project_id", projectId)
                req.put("caller_commit_date", commitDate)
                req.put("caller_commit_sha1", commitSha1)
                req.put("caller_line_number", it.underlyingNode.startPosition.line())
                req.put("caller_function_id", functionIds.get(it.parentSourceNode.attributes.get("kytheUri")))
                req.put("callee_function_id", functionIds.get(it.attributes.get("calledUri")))
                request.add(req)
            }

            def future = Future.future()
            futures << future
            client.post(port, host, "/api/references")
                    .expect(ResponsePredicate.SC_OK)
                    .bearerTokenAuthentication(apiKey).sendJson(request, {
                if (it.succeeded()) {
                    future.complete()
                } else {
                    future.fail(it.cause())
                }
            })
        }
        CompositeFuture.all(futures).setHandler({
            if (it.succeeded()) {
                handler.handle(Future.succeededFuture())
            } else {
                handler.handle(Future.failedFuture(it.cause()))
            }
        })
    }
}
