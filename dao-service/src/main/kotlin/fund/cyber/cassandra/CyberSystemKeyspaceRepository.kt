package fund.cyber.cassandra

import com.datastax.driver.core.Cluster
import fund.cyber.node.model.SchemaVersion

class CyberSystemKeyspaceRepository(cassandra: Cluster)
    : CassandraKeyspaceRepository(cassandra, "cyber_system") {

    val schemaVersionMapper by lazy { mappingManager.mapper(SchemaVersion::class.java)!! }

    fun getLastMigration(applicationId: String): SchemaVersion? {
        val resultSet = session.execute("SELECT * FROM schema_version WHERE application_id='$applicationId' LIMIT 1")
        return schemaVersionMapper.map(resultSet).firstOrNull()
    }
}