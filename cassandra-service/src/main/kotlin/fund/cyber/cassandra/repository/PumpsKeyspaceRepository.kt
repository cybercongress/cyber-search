package fund.cyber.cassandra.repository

import com.datastax.driver.core.Cluster
import fund.cyber.cassandra.CassandraKeyspaceRepository
import fund.cyber.node.model.IndexingProgress


class PumpsKeyspaceRepository(cassandra: Cluster)
    : CassandraKeyspaceRepository(cassandra, "pump") {

    val indexingProgressStore by lazy { mappingManager.mapper(IndexingProgress::class.java)!! }

    fun getLastMigration(applicationId: String): IndexingProgress? {
        val resultSet = session.execute("SELECT * FROM schema_version WHERE application_id='$applicationId' LIMIT 1")
        return indexingProgressStore.map(resultSet).firstOrNull()
    }
}
