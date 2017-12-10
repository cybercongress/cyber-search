package fund.cyber.cassandra.migration

import com.datastax.driver.core.Session
import com.datastax.driver.mapping.MappingManager
import fund.cyber.cassandra.CassandraService
import fund.cyber.cassandra.keyspace
import fund.cyber.node.common.readAsString
import fund.cyber.node.model.SchemaVersion
import org.apache.http.HttpStatus
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.client.utils.HttpClientUtils
import org.slf4j.LoggerFactory
import java.net.URI
import java.util.*
import fund.cyber.node.common.Chain
import kotlinx.coroutines.experimental.runBlocking

class ElassandraSchemaMigrationEngine(
        elasticHost: String,
        elasticPort: Int,
        private val cassandraService: CassandraService,
        private val httpClient: HttpClient,
        private val defaultMigrations: List<Migration> = emptyList()
) {


    private val systemKeyspaceRepository = cassandraService.systemKeyspaceRepository
    private val elasticBaseUri = URI.create("http://$elasticHost:$elasticPort")
    private val log = LoggerFactory.getLogger(ElassandraSchemaMigrationEngine::class.java)


    fun executeSchemaUpdate(migrations: List<Migration>, chain: Chain) {

        log.info("Executing elassandra schema update")
        val session: Session = cassandraService.newSession(chain.keyspace)

        defaultMigrations.plus(migrations).groupBy { m -> m.applicationId }.forEach { applicationId, applicationMigrations ->

            val lastMigrationVersion = systemKeyspaceRepository.getLastMigration(applicationId)?.version ?: -1

            applicationMigrations
                    .filter { migration -> migration.version > lastMigrationVersion }
                    .sortedBy { migration -> migration.version }
                    .forEach { migration ->
                        log.info("Executing '$applicationId' application migration to '${migration.version}' version")
                        executeMigration(migration, session)
                        log.info("Succeeded '$applicationId' application migration to '${migration.version}' version")
                    }

        }

//        session.closeAsync()
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
            is CassandraEntityMigration -> {
                val manager = MappingManager(session)

                runBlocking {
                    migration.entities.map { (cls, entity) ->
                        val mapper = manager.mapper(cls)
                        mapper.saveAsync(cls.cast(entity))
                    }
                }
            }
        }

        val schemeMigrationRecord = SchemaVersion(
                application_id = migration.applicationId, version = migration.version,
                apply_time = Date(), migration_hash = migration.hashCode()
        )
        systemKeyspaceRepository.schemaVersionMapper.save(schemeMigrationRecord)
    }
}