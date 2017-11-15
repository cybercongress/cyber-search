package fund.cyber.pump.cassandra

import com.datastax.driver.core.Session

/**
 * Repository to handle the Cassandra schema.
 *
 */
class KeyspaceRepository(private val session: Session) {

    /**
     * Method used to create any keyspace - schema.
     *
     * @param schemaName the name of the schema.
     * @param replicatioonStrategy the replication strategy.
     * @param numberOfReplicas the number of replicas.
     */
    fun createKeyspace(keyspaceName: String, replicatioonStrategy: String, numberOfReplicas: Int) {
        val sb = StringBuilder("CREATE KEYSPACE IF NOT EXISTS ").append(keyspaceName).append(" WITH replication = {").append("'class':'").append(replicatioonStrategy).append("','replication_factor':").append(numberOfReplicas).append("};")

        val query = sb.toString()

        session.execute(query)
    }

    fun useKeyspace(keyspace: String) {
        session.execute("USE " + keyspace)
    }

    /**
     * Method used to delete the specified schema.
     * It results in the immediate, irreversable removal of the keyspace, including all tables and data contained in the keyspace.
     *
     * @param schemaName the name of the keyspace to delete.
     */
    fun deleteKeyspace(keyspaceName: String) {
        val sb = StringBuilder("DROP KEYSPACE ").append(keyspaceName)

        val query = sb.toString()

        session.execute(query)
    }
}