package fund.cyber.dao.system

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.mapping.MappingManager
import fund.cyber.node.model.SchemaVersion

class SystemDaoService(cassandra: Cluster) {

    private val session: Session = cassandra.connect("cyber_system")

    private val mappingManager = MappingManager(session)
    val schemaVersionMapper = mappingManager.mapper(SchemaVersion::class.java)!!

    fun getLastMigration(applicationId: String): SchemaVersion? {
        val resultSet = session.execute("SELECT * FROM schema_version WHERE application_id='$applicationId' LIMIT 1")
        return schemaVersionMapper.map(resultSet).firstOrNull()
    }
}