package io.gitdetective.tools

import ai.grakn.Grakn
import ai.grakn.GraknSession
import ai.grakn.GraknTxType
import ai.grakn.graql.internal.query.QueryAnswer

/**
 * @author <a href="mailto:brandon.fergerson@codebrig.com">Brandon Fergerson</a>
 */
class GetProjects {

    private static final String graknHost = "localhost"
    private static final int graknPort = 4567
    private static final String graknKeyspace = "grakn"
    private static final GraknSession session = Grakn.session(graknHost + ":" + graknPort, graknKeyspace)

    static void main(String[] args) {
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
