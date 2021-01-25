package io.gitdetective.indexer.stage

import io.gitdetective.indexer.extractor.MavenReferenceExtractor
import io.gitdetective.web.dao.JobsDAO
import io.vertx.blueprint.kue.Kue
import io.vertx.blueprint.kue.queue.Job
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.errors.TransportException
import org.kohsuke.github.GHFileNotFoundException
import org.kohsuke.github.GitHub

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

import static io.gitdetective.indexer.IndexerServices.logPrintln

/**
 * Determines if project should be indexed.
 * If so, clones and sets project up for building
 *
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GithubRepositoryCloner extends AbstractVerticle {

    public static final String INDEX_GITHUB_PROJECT_JOB_TYPE = "IndexGithubProject"
    private final static Logger log = LoggerFactory.getLogger(GithubRepositoryCloner.class)
    private static final int CLONE_TIMEOUT_LIMIT_MINUTES = 20
    private static final int NEEDED_GITHUB_HIT_COUNT = 6
    private static final int GITHUB_RATE_LIMIT_WAIT_MINUTES = 15
    private final Kue kue
    private final JobsDAO jobs
    private GitHub github

    GithubRepositoryCloner(Kue kue, JobsDAO jobs) {
        this.kue = kue
        this.jobs = jobs
    }

    @Override
    void start() throws Exception {
        github = GitHub.connectUsingOAuth(config().getString("oauth_token"))
        log.info "Connected to GitHub: " + github.credentialValid

        kue.on("error", {
            log.error "Indexer job error: " + it.body()
        })
        kue.process(INDEX_GITHUB_PROJECT_JOB_TYPE, config().getInteger("builder_thread_count"), { job ->
            def githubRepository = job.data.getString("github_repository").toLowerCase()

            //skip build if already done in last 24 hours
            jobs.getProjectLastBuilt(githubRepository, {
                if (it.failed()) {
                    it.cause().printStackTrace()
                } else {
                    try {
                        boolean skippingDownload = false
                        def result = it.result() as String
                        if (result != null) {
                            def lastBuild = Instant.parse(result)
                            if (lastBuild.plus(24, ChronoUnit.HOURS).isAfter(Instant.now())) {
                                if (!job.data.getBoolean("admin_triggered")) {
                                    skippingDownload = true
                                    logPrintln(job, "Skipping project download and build. Limited to once a day")
                                }
                            }
                        }

                        if (skippingDownload) {
                            job.done()
                        } else {
                            vertx.executeBlocking({
                                try {
                                    downloadAndExtractProject(job, githubRepository)
                                    it.complete()
                                } catch (GHFileNotFoundException ex) {
                                    logPrintln(job, "Could not locate project on GitHub")
                                    it.fail(ex)
                                } catch (all) {
                                    it.fail(all)
                                }
                            }, false, {
                                if (it.failed()) {
                                    job.done(it.cause())
                                }
                            })
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace()
                        logPrintln(job, ex.getMessage())
                        job.done(ex)
                    }
                }
            })
        })
    }

    private void downloadAndExtractProject(Job job, String githubRepository) throws IOException {
        def rateLimit = github.getRateLimit()
        if (rateLimit.remaining <= NEEDED_GITHUB_HIT_COUNT) {
            log.info "Current limit: " + rateLimit.remaining + "; Waiting $GITHUB_RATE_LIMIT_WAIT_MINUTES minutes"
            Thread.sleep(TimeUnit.MINUTES.toMillis(GITHUB_RATE_LIMIT_WAIT_MINUTES))
            log.info "Finishing waiting"
        } else {
            log.debug "Current rate limit remaining: " + rateLimit.remaining
        }

        logPrintln(job, "Fetching project data from GitHub")
        def repo = github.getRepository(githubRepository)

        //skip forked projects (queue parent project)
        if (repo.fork) {
            if (!config().getBoolean("create_forked_project_parent_jobs")) {
                logPrintln(job, "Forked projects not currently supported. Skipped build")
                job.done()
                return
            } else {
                logPrintln(job, "Forked projects not fully supported. Building parent")
            }
            def parent = repo.parent
            def parentGithubRepository = parent.fullName.toLowerCase()

            jobs.getProjectLastQueued(parentGithubRepository, {
                if (it.failed()) {
                    job.done(it.cause())
                    return
                }

                logPrintln(job, "Checking parent project status")
                if (it.result().isPresent()) {
                    def lastQueue = it.result().get()
                    if (lastQueue.plus(24, ChronoUnit.HOURS).isAfter(Instant.now())) {
                        //parent project already queued in last 24 hours; ignore
                        logPrintln(job, "Parent project already queued")
                        job.done()
                        return
                    }
                }

                //queue parent project build
                def data = new JsonObject().put("github_repository", parentGithubRepository)
                jobs.createJob(INDEX_GITHUB_PROJECT_JOB_TYPE, "System build job queued",
                        data, job.priority, {
                    if (it.failed()) {
                        job.done(it.cause())
                    } else {
                        def parentProjectName = it.result().data.getString("github_repository")
                        log.info "Forked project created job: " + it.result().id + " - Parent: " + parentProjectName
                        logPrintln(job, "Queued build for parent project: " + parentProjectName)
                        job.done()
                    }
                })
            })
            return
        }

        logPrintln(job, "Detecting project build system")
        def mavenProject = false
        def gradleProject = false
        repo.getDirectoryContent("/").each {
            if (it.name == "pom.xml") {
                mavenProject = true
            } else if (it.name == "build.gradle") {
                gradleProject = true
            }
        }

        if (mavenProject || gradleProject) {
            String builderAddress
            if (mavenProject) {
                logPrintln(job, "Detected Maven build system")
                builderAddress = MavenReferenceExtractor.EXTRACTOR_ADDRESS
            } else {
//                logPrintln(job, "Detected Gradle build system")
//                builderAddress = KytheGradleBuilder.BUILDER_ADDRESS
            }
            def latestCommit = repo.getBranch(repo.getDefaultBranch()).SHA1
            def latestCommitDate = repo.getCommit(latestCommit).commitDate.toInstant()

            //skip build if last built commit is same as current commit
            jobs.getProjectLastIndexedCommitInformation(githubRepository, {
                if (it.failed()) {
                    job.done(it.cause())
                } else {
                    boolean skippingBuild = false
                    def result = it.result() as String
                    if (result != null) {
                        def lastCommitInfo = new JsonObject(result)
                        if (lastCommitInfo.getString("commit") == latestCommit) {
                            if (!job.data.getBoolean("admin_triggered")) {
                                skippingBuild = true
                                logPrintln(job, "Skipping project build. Already built latest commit")
                            }
                        }
                    }

                    if (skippingBuild) {
                        job.done()
                    } else {
                        cloneAndBuildProject(job, githubRepository, latestCommit, latestCommitDate, builderAddress)
                        jobs.setProjectLastBuilt(githubRepository, Instant.now(), {
                            //nothing
                        })
                    }
                }
            })
        } else {
            logPrintln(job, "Skipping project build. Couldn't detect supported build system")
            job.done()
        }
    }

    private void cloneAndBuildProject(Job job, String githubRepository, String latestCommit, Instant latestCommitDate,
                                      String builderAddress) {
        //clean output directory
        def outputDirectory = new File(config().getString("temp_directory"), UUID.randomUUID().toString())
        outputDirectory.deleteDir()
        outputDirectory.mkdirs()

        vertx.executeBlocking({ blocking ->
            logPrintln(job, "Cloning project to local filesystem")
            try {
                Git.cloneRepository()
                        .setURI("https://github.com/" + githubRepository + ".git")
                        .setDirectory(outputDirectory)
                        .setCloneSubmodules(true)
                        .setTimeout(TimeUnit.MINUTES.toSeconds(CLONE_TIMEOUT_LIMIT_MINUTES) as int)
                        .call()

                if (MavenReferenceExtractor.EXTRACTOR_ADDRESS == builderAddress) {
                    File mavenBuildPomFile = new File(outputDirectory, "pom.xml")
                    if (mavenBuildPomFile.exists()) {
                        logPrintln(job, "Project successfully cloned")
                        blocking.complete(mavenBuildPomFile)
                    } else {
                        logPrintln(job, "Failed to find pom.xml")
                        blocking.fail("Failed to find pom.xml")
                    }
                }
//                else if (KytheGradleBuilder.BUILDER_ADDRESS == builderAddress) {
//                    File gradleBuildFile = new File(outputDirectory, "build.gradle")
//                    if (gradleBuildFile.exists()) {
//                        logPrintln(job, "Project successfully cloned")
//                        blocking.complete(gradleBuildFile)
//                    } else {
//                        logPrintln(job, "Failed to find build.gradle")
//                        blocking.fail("Failed to find build.gradle")
//                    }
//                }
                else {
                    throw new IllegalArgumentException("Unsupported builder: $builderAddress")
                }
            } catch (TransportException e) {
                logPrintln(job, "Project clone timed out")
                blocking.fail("Project clone timed out")
            }
        }, false, { res ->
            if (res.failed()) {
                job.done(res.cause())
            } else {
                job.data.put("commit", latestCommit)
                job.data.put("commit_date", latestCommitDate)
                job.data.put("output_directory", outputDirectory.absolutePath)
                job.data.put("build_target", (res.result() as File).absolutePath)
                job.save().onComplete({
                    if (it.succeeded()) {
                        vertx.eventBus().send(builderAddress, job)
                    } else {
                        job.done(it.cause())
                    }
                })
            }
        })
    }
}
