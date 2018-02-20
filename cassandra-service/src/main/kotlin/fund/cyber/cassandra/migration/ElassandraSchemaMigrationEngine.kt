package fund.cyber.cassandra.migration

import fund.cyber.cassandra.migration.configuration.MigrationRepositoryConfiguration
import fund.cyber.cassandra.migration.model.CqlSchemaVersion
import fund.cyber.cassandra.migration.repository.SchemaVersionRepository
import fund.cyber.search.common.readAsString
import fund.cyber.search.configuration.*
import org.apache.http.HttpStatus
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.client.utils.HttpClientUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.cassandra.core.ReactiveCassandraOperations
import org.springframework.stereotype.Component
import java.net.URI
import java.util.*

private const val SCHEMA_VERSION_CQL_LOCATION = "/migrations/schema_version_create.cql"

@Component
class ElassandraSchemaMigrationEngine(
        @Value("\${$CASSANDRA_HOSTS:$CASSANDRA_HOSTS_DEFAULT}")
        private val cassandraHosts: String,
        @Value("\${$ELASTIC_HTTP_PORT:$ELASTIC_HTTP_PORT_DEFAULT}")
        private val elasticPort: Int,
        private val httpClient: HttpClient,
        @Qualifier("keyspaceMigrationCassandraTemplate")
        private val cassandraTemplate: ReactiveCassandraOperations,
        private val schemaVersionRepository: SchemaVersionRepository,
        private val migrationsLoader: MigrationsLoader,
        private val migrationSettings: MigrationSettings = defaultSettings
) : InitializingBean {

    private val elasticBaseUri = URI.create("http://${cassandraHosts.split(",").first()}:$elasticPort")
    private val log = LoggerFactory.getLogger(ElassandraSchemaMigrationEngine::class.java)

    override fun afterPropertiesSet() {
        createSchemaVersionTable()
        executeSchemaUpdate(migrationsLoader.load(migrationSettings))
    }

    private fun executeSchemaUpdate(migrations: List<Migration>) {

        log.info("Executing elassandra schema update")
        log.info("Found ${migrations.size} migrations")

        migrations.filter { it !is EmptyMigration }.groupBy { m -> m.applicationId }.forEach { applicationId, applicationMigrations ->

            val executedMigrations = schemaVersionRepository.findAllByApplicationId(applicationId)
                    .map(CqlSchemaVersion::id).collectList().block() ?: emptyList()

            applicationMigrations
                    .filter { migration -> !executedMigrations.contains(migration.id) }
                    .sortedBy { migration -> migration.id.substringBefore("_").toInt() }
                    .forEach { migration ->
                        log.info("Executing '$applicationId' application migration to '${migration.id}' id")
                        executeMigration(migration)
                        log.info("Succeeded '$applicationId' application migration to '${migration.id}' id")
                    }

        }

        log.info("Elassandra schema update done")
    }

    @Suppress("UNCHECKED_CAST")
    private fun executeMigration(migration: Migration) {

        when (migration) {
            is CassandraMigration -> migration.getStatements().forEach { statement -> cassandraTemplate.reactiveCqlOperations.execute(statement).block() }
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

        val schemeMigrationRecord = CqlSchemaVersion(
                applicationId = migration.applicationId, id = migration.id,
                apply_time = Date(), migration_hash = migration.hashCode()
        )
        schemaVersionRepository.save(schemeMigrationRecord).block()
    }

    private fun createSchemaVersionTable() {

        val createSchemaVersionCql = MigrationRepositoryConfiguration::class.java
                .getResourceAsStream(SCHEMA_VERSION_CQL_LOCATION).readAsString()

        cassandraTemplate.reactiveCqlOperations.execute(createSchemaVersionCql).block()
    }


}