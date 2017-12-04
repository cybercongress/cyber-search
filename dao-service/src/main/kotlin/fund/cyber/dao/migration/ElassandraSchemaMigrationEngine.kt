package fund.cyber.dao.migration

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import fund.cyber.dao.system.SystemDaoService
import fund.cyber.node.common.readAsString
import fund.cyber.node.model.SchemaVersion
import org.apache.http.HttpStatus
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.client.utils.HttpClientUtils
import org.slf4j.LoggerFactory
import java.net.URI
import java.util.*

class ElassandraSchemaMigrationEngine(
        elasticHost: String,
        elasticPort: Int,
        private val cassandra: Cluster,
        private val httpClient: HttpClient,
        private val systemDaoService: SystemDaoService,
        private val defaultMigrations: List<Migration> = emptyList()
) {

    private val elasticBaseUri = URI.create("http://$elasticHost:$elasticPort")
    private val log = LoggerFactory.getLogger(ElassandraSchemaMigrationEngine::class.java)


    fun executeSchemaUpdate(migrations: List<Migration>) {

        log.info("Executing elassandra schema update")
        val session: Session = cassandra.connect()

        defaultMigrations.plus(migrations).groupBy { m -> m.applicationId }.forEach { applicationId, applicationMigrations ->

            val lastMigrationVersion = systemDaoService.getLastMigration(applicationId)?.version ?: -1

            applicationMigrations
                    .filter { migration -> migration.version > lastMigrationVersion }
                    .sortedBy { migration -> migration.version }
                    .forEach { migration ->
                        log.info("Executing '$applicationId' application migration to '${migration.version}' version")
                        executeMigration(migration, session)
                        log.info("Succeeded '$applicationId' application migration to '${migration.version}' version")
                    }

        }

        session.closeAsync()
        log.info("Elassandra schema update done")
    }


    private fun executeMigration(migration: Migration, session: Session) {

        when (migration) {
            is CassandraMigration -> {
                migration.getStatements().forEach { statement -> session.execute(statement) }
            }
            is ElasticHttpMigration -> {

                val requestWithRelativeUri = migration.getRequest()
                val fullUri = elasticBaseUri.resolve(requestWithRelativeUri.uri)
                val request = RequestBuilder.copy(requestWithRelativeUri).setUri(fullUri).build()
                val response = httpClient.execute(request)

                try {
                    if (response.statusLine.statusCode != HttpStatus.SC_OK) {
                        throw RuntimeException(response.entity.content.readAsString())
                    }
                } finally {
                    HttpClientUtils.closeQuietly(response)
                }

            }
        }

        val schemeMigrationRecord = SchemaVersion(
                application_id = migration.applicationId, version = migration.version,
                apply_time = Date(), migration_hash = migration.hashCode()
        )
        systemDaoService.schemaVersionMapper.save(schemeMigrationRecord)
    }
}