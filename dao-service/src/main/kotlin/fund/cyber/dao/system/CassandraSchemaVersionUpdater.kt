package fund.cyber.dao.system

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import fund.cyber.node.model.SchemaVersion
import org.slf4j.LoggerFactory
import java.util.*

class CassandraSchemaVersionUpdater(
        private val cassandra: Cluster,
        private val systemDaoService: SystemDaoService,
        private val migrations: List<Migration>
) {

    private val log = LoggerFactory.getLogger(CassandraSchemaVersionUpdater::class.java)


    fun executeSchemaUpdate() {

        log.info("Executing schema update")
        val session: Session = cassandra.connect()

        migrations.groupBy { m -> m.applicationId }.forEach { applicationId, applicationMigrations ->

            val lastMigrationVersion = systemDaoService.getLastMigration(applicationId)?.version ?: -1

            applicationMigrations
                    .filter { migration -> migration.version > lastMigrationVersion }
                    .sortedBy { migration -> migration.version }
                    .forEach { migration ->

                        log.info("Executing '$applicationId' application migration to '${migration.version}' version")
                        migration.execute(session)

                        val schemeMigrationRecord = SchemaVersion(
                                application_id = applicationId, version = migration.version,
                                apply_time = Date(), migration_hash = migration.hashCode()
                        )
                        systemDaoService.schemaVersionMapper.save(schemeMigrationRecord)
                        log.info("Succeeded '$applicationId' application migration to '${migration.version}' version")
                    }

        }

        session.closeAsync()
        log.info("Schema update done")
    }
}