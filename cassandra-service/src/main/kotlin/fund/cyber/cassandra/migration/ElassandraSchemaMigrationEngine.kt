package fund.cyber.cassandra.migration

import com.datastax.driver.core.Session
import com.datastax.driver.mapping.Mapper
import fund.cyber.cassandra.CassandraService
import fund.cyber.cassandra.PREFERRED_CONCURRENT_REQUEST_TO_SAVE_ENTITIES_LIST
import fund.cyber.node.common.awaitAll
import fund.cyber.node.common.readAsString
import fund.cyber.node.model.CyberSearchItem
import fund.cyber.node.model.SchemaVersion
import io.reactivex.Flowable
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
        private val cassandraService: CassandraService,
        private val httpClient: HttpClient,
        private val defaultMigrations: List<Migration> = emptyList()
) {


    private val systemKeyspaceRepository = cassandraService.systemKeyspaceRepository
    private val elasticBaseUri = URI.create("http://$elasticHost:$elasticPort")
    private val log = LoggerFactory.getLogger(ElassandraSchemaMigrationEngine::class.java)


    fun executeSchemaUpdate(migrations: List<Migration>) {

        log.info("Executing elassandra schema update")
        val session: Session = cassandraService.newSession()

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

        session.close()
        log.info("Elassandra schema update done")
    }


    @Suppress("UNCHECKED_CAST")
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

                val manager = cassandraService.getChainRepository(migration.chain).mappingManager

                Flowable.fromIterable(migration.entities)
                        .buffer(PREFERRED_CONCURRENT_REQUEST_TO_SAVE_ENTITIES_LIST)
                        .blockingForEach { entities ->
                            entities.map { entity ->
                                (manager.mapper(entity::class.java) as Mapper<CyberSearchItem>).saveAsync(entity)
                            }.awaitAll()
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