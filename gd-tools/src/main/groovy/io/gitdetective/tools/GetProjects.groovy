package io.gitdetective.tools

import ai.grakn.Grakn
import ai.grakn.GraknSession
import ai.grakn.GraknTxType
import ai.grakn.graql.internal.query.QueryAnswer
import io.vertx.core.json.JsonObject
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GetProjects {

    static void main(String[] args) {
        def configFile = new File("web-config.json")
        if (!configFile.exists()) {
            throw new IllegalStateException("Missing web-config.json")
        }
        def config = new JsonObject(IOUtils.toString(configFile.newInputStream(), StandardCharsets.UTF_8))
        if (!config.getBoolean("grakn.enabled")) {
            throw new IllegalStateException("Grakn is disabled")
        }

        GraknSession session = Grakn.session(config.getString("grakn.host") + ":" +
                config.getInteger("grakn.port"), config.getString("grakn.keyspace"))
        def tx = session.open(GraknTxType.READ)
        def graql = tx.graql()
        def query = graql.parse('match $x isa project has project_name $name; get $name;')
        def answers = query.execute() as List<QueryAnswer>
        if (answers.isEmpty()) {
            println "Found no projects"
        } else {
            println "Found projects:"
            for (def answer : answers) {
                def projectName = answer.get("name").asAttribute().value.toString()
                println projectName
            }
        }
        tx.close()
        session.close()
    }

}
