package io.gitdetective.web.work

import io.gitdetective.web.dao.JobsDAO
import io.reactivex.Observable
import io.reactivex.ObservableEmitter
import io.reactivex.ObservableOnSubscribe
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.*
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory

import java.time.Instant
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPInputStream

/**
 * Syncs with GH Archive to find projects to index
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GHArchiveSync extends AbstractVerticle {

    public final static String STANDALONE_MODE = "GHArchiveStandaloneMode"
    private final static Logger log = LoggerFactory.getLogger(GHArchiveSync.class)
    private final JobsDAO jobs

    GHArchiveSync(JobsDAO jobs) {
        this.jobs = jobs
    }

    @Override
    void start() throws Exception {
        if (!config().getBoolean("gh_sync_standalone_mode", false)) {
            syncGithubArchive()

            vertx.setPeriodic(TimeUnit.HOURS.toMillis(1), new Handler<Long>() {
                void handle(Long aLong) {
                    syncGithubArchive()
                }
            })
        } else {
            vertx.eventBus().consumer(STANDALONE_MODE, { standalone ->
                def syncRequest = standalone.body() as JsonObject
                def fromDate = LocalDate.parse(syncRequest.getString("from_date"))
                def toDate = LocalDate.parse(syncRequest.getString("to_date"))

                def futures = new ArrayList<Future>()
                while (fromDate.isEqual(toDate) || fromDate.isBefore(toDate)) {
                    for (int i = 0; i < 23; i++) {
                        def archiveFile = fromDate.toString() + "-$i"
                        def dlFile = downloadArchive(archiveFile)
                        if (dlFile != null) {
                            def fut = Future.future()
                            futures.add(fut)
                            processArchive(dlFile, archiveFile, fut.completer())
                        }
                    }
                    fromDate = fromDate.plusDays(1)
                }

                CompositeFuture.all(futures).setHandler({
                    if (it.failed()) {
                        it.cause().printStackTrace()
                        standalone.fail(-1, it.cause().message)
                    } else {
                        standalone.reply(true)
                    }
                })
            })
        }
    }

    private void syncGithubArchive() {
        if (!config().getBoolean("github_archive_sync_enabled")) {
            log.info "GitHub Archive sync disabled. Skipping sync"
            return
        }

        log.info "Syncing GitHub Archive"
        jobs.getLastArchiveSync({
            if (it.failed()) {
                it.cause().printStackTrace()
                return
            }

            def result = it.result()
            def nextStr = ""
            if (result == null) {
                //start first of today
                nextStr = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE) + "-0"
            } else {
                //start with next (if available)
                def dateStr = result.substring(0, result.lastIndexOf("-"))
                int currentHour = result.substring(result.lastIndexOf("-") + 1) as int
                if (currentHour < 23) {
                    //next hour
                    nextStr = dateStr + "-" + (currentHour + 1)
                } else {
                    //next day
                    nextStr = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE)
                            .plusDays(1).format(DateTimeFormatter.ISO_LOCAL_DATE) + "-0"
                }
            }

            def dlFile = downloadArchive(nextStr)
            if (dlFile != null) {
                jobs.setLastArchiveSync(nextStr, {
                    if (it.failed()) {
                        it.cause().printStackTrace()
                    }

                    processArchive(dlFile, nextStr, {
                        //do nothing
                    })
                })
            }
        })
    }

    private void processArchive(File dlFile, String archiveFile, Handler<AsyncResult> handler) {
        def tempDir = config().getString("temp_directory")
        def jsonFile = new File(tempDir, archiveFile + ".json")
        gunzip(dlFile, jsonFile)
        dlFile.delete()
        log.debug "Extracted archive data"

        int lineNumber = 0
        Set<String> interestedRepos = new HashSet<String>()
        jsonFile.eachLine {
            try {
                def ob = new JsonObject(it)
                if (ob.getString("type") == "PullRequestEvent") {
                    def repoName = ob.getJsonObject("repo").getString("name")
                    def baseRepo = ob.getJsonObject("payload").getJsonObject("pull_request")
                            .getJsonObject("base").getJsonObject("repo")
                    //def stargazersCount = baseRepo.getInteger("stargazers_count")
                    def language = baseRepo.getString("language")

                    switch (language?.toLowerCase()) {
                        case "groovy":
                        case "java":
                        case "kotlin":
                        case "scala":
                        case "closure":
                            interestedRepos.add(repoName)
                    }
                }
            } catch (all) {
                all.printStackTrace()
            }

            if (++lineNumber % 10000 == 0) {
                log.debug "Found " + interestedRepos.size() + " interested repos of " + lineNumber + " line entries"
            }
        }
        log.info "Found " + interestedRepos.size() + " interested repos"

        def jobsCreated = new AtomicInteger(0)
        def observables = new ArrayList<Observable>()
        interestedRepos.each {
            observables.add(asyncWork(it.toLowerCase(), jobsCreated))
        }
        Observable.concat(Observable.fromIterable(observables)).subscribe(
                {}, { it.printStackTrace() },
                { log.info "Jobs created: " + jobsCreated.get(); handler.handle(Future.succeededFuture()) }
        )
    }

    private File downloadArchive(String archiveFile) {
        log.info "Downloading GHArchive file: " + archiveFile
        def tempDir = config().getString("temp_directory")
        def url = "http://data.gharchive.org/" + archiveFile + ".json.gz"
        def dlFile = new File(tempDir, archiveFile + ".json.gz")
        dlFile.delete()
        dlFile.parentFile.mkdirs()

        try {
            def file = dlFile.newOutputStream()
            def httpConn = (HttpURLConnection) new URL(url).openConnection()
            httpConn.addRequestProperty("User-Agent", "Mozilla/4.0")
            file << httpConn.getInputStream()
            file.close()
        } catch (all) {
            log.error "$archiveFile archive file not available yet"
            return null
        }

        log.info "Downloaded archive file: $archiveFile"
        return dlFile
    }

    private Observable<Job> asyncWork(String repo, AtomicInteger jobsCreated) {
        return Observable.create(new ObservableOnSubscribe<Job>() {
            @Override
            void subscribe(ObservableEmitter<Job> emitter) throws Exception {
                jobs.getProjectLastQueued(repo, {
                    if (it.failed()) {
                        it.cause().printStackTrace()
                        emitter.onError(it.cause())
                        return
                    }

                    if (it.result().isPresent()) {
                        def lastBuild = it.result().get()
                        if (lastBuild.plus(24, ChronoUnit.HOURS).isAfter(Instant.now())) {
                            //already built in last 24 hours; ignore
                            emitter.onComplete()
                            return
                        }
                    }

                    //add to build queue
                    jobs.createJob("IndexGithubProject",
                            "System build job queued", repo, {
                        if (it.failed()) {
                            it.cause().printStackTrace()
                            emitter.onError(it.cause())
                        } else {
                            jobsCreated.incrementAndGet()
                            emitter.onComplete()
                        }
                    })
                })
            }
        })
    }

    static void gunzip(File inputFile, File outputFile) throws IOException {
        byte[] buffer = new byte[1024]
        GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(inputFile))
        FileOutputStream out = new FileOutputStream(outputFile)

        int len
        while ((len = gzis.read(buffer)) > 0) {
            out.write(buffer, 0, len)
        }
        gzis.close()
        out.close()
    }
}
