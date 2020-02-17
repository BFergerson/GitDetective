package io.gitdetective.tools

import io.gitdetective.web.dao.JobsDAO
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets
import java.time.LocalDate

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class FindProjects extends AbstractVerticle {

    static LocalDate fromDate
    static LocalDate toDate

    static void main(String[] args) {
        def configFile = new File("web-config.json")
        if (!configFile.exists()) {
            throw new IllegalStateException("Missing web-config.json")
        } else if (args.length < 2) {
            throw new IllegalArgumentException("Invalid arguments: " + args.toArrayString())
        }
        fromDate = LocalDate.parse(args[0])
        toDate = LocalDate.parse(args[1])
        if (toDate.isBefore(fromDate)) {
            throw new IllegalArgumentException("Invalid date range: $fromDate - $toDate")
        }

        def config = new JsonObject(IOUtils.toString(configFile.newInputStream(), StandardCharsets.UTF_8))
        def vertxOptions = new VertxOptions()
        vertxOptions.setBlockedThreadCheckInterval(Integer.MAX_VALUE)
        Vertx.vertx(vertxOptions).deployVerticle(new FindProjects(), new DeploymentOptions().setConfig(config), {
            if (it.failed()) {
                it.cause().printStackTrace()
                System.exit(-1)
            }
        })
    }

    @Override
    void start() throws Exception {
        def jobs = new JobsDAO(vertx, config())
//        vertx.deployVerticle(new GHArchiveSync(jobs), new DeploymentOptions()
//                .setConfig(config().put("gh_sync_standalone_mode", true)), {
//            if (it.failed()) {
//                it.cause().printStackTrace()
//                System.exit(-1)
//            }
//
//            vertx.eventBus().send(GHArchiveSync.STANDALONE_MODE, new JsonObject()
//                    .put("from_date", fromDate.toString())
//                    .put("to_date", toDate.toString()), new DeliveryOptions().setSendTimeout(Integer.MAX_VALUE), {
//                if (it.failed()) {
//                    it.cause().printStackTrace()
//                    System.exit(-1)
//                }
//
//                vertx.close()
//            })
//        })
    }

}
