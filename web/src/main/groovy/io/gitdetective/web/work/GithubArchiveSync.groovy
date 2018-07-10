package io.gitdetective.web.work

import io.gitdetective.indexer.stage.GithubRepositoryCloner
import io.gitdetective.web.dao.JobsDAO
import io.gitdetective.web.dao.RedisDAO
import io.reactivex.Observable
import io.reactivex.ObservableEmitter
import io.reactivex.ObservableOnSubscribe
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.Handler
import io.vertx.core.json.JsonObject

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
class GithubArchiveSync extends AbstractVerticle {

    private final JobsDAO jobs
    private final RedisDAO redis

    GithubArchiveSync(JobsDAO jobs, RedisDAO redis) {
        this.jobs = jobs
        this.redis = redis
    }

    @Override
    void start() throws Exception {
        syncGithubArchive()

        vertx.setPeriodic(TimeUnit.HOURS.toMillis(1), new Handler<Long>() {
            void handle(Long aLong) {
                syncGithubArchive()
            }
        })
    }

    private void syncGithubArchive() {
        if (!config().getBoolean("github_archive_sync_enabled")) {
            println "GitHub Archive sync disabled. Skipping sync"
            return
        }

        println "Syncing GitHub Archive"
        redis.getLastArchiveSync({
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
                redis.setLastArchiveSync(nextStr, {
                    if (it.failed()) {
                        it.cause().printStackTrace()
                    }

                    processArchive(dlFile, nextStr)
                })
            }
        })
    }

    private void processArchive(File dlFile, String archiveFile) {
        def tempDir = config().getString("temp_directory")
        def jsonFile = new File(tempDir, archiveFile + ".json")
        gunzip(dlFile, jsonFile)
        dlFile.delete()
        println "Extracted archive data"

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
                println "Found " + interestedRepos.size() + " interested repos of " + lineNumber + " line entries"
            }
        }
        println "Found " + interestedRepos.size() + " interested repos"

        def jobsCreated = new AtomicInteger(0)
        def observables = new ArrayList<Observable>()
        interestedRepos.each {
            observables.add(asyncWork(it.toLowerCase(), jobsCreated))
        }
        Observable.concat(Observable.fromIterable(observables)).subscribe(
                {}, { it.printStackTrace() },
                { println "Jobs created: " + jobsCreated.get() }
        )
    }

    private File downloadArchive(String archiveFile) {
        println "Downloading GitHub archive file: " + archiveFile
        def tempDir = config().getString("temp_directory")
        def url = "http://data.gharchive.org/" + archiveFile + ".json.gz"
        def dlFile = new File(tempDir, archiveFile + ".json.gz")
        dlFile.delete()
        dlFile.parentFile.mkdirs()

        try {
            def file = dlFile.newOutputStream()
            def httpcon = (HttpURLConnection) new URL(url).openConnection()
            httpcon.addRequestProperty("User-Agent", "Mozilla/4.0")
            file << httpcon.getInputStream()
            file.close()
        } catch (all) {
            println "$archiveFile archive file not available yet"
            return null
        }

        println "Downloaded archive data"
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
                    jobs.createJob(GithubRepositoryCloner.INDEX_GITHUB_PROJECT_JOB_TYPE,
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
