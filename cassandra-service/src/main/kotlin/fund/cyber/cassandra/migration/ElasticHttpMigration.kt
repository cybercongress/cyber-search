package fund.cyber.cassandra.migration

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.http.HttpRequest
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.entity.ByteArrayEntity
import java.net.URI

interface ElasticMigration : Migration {

    /**
     * Returns request with relative url. Migration Engine that add host:port prefix to fulfill uri.
     */
    fun getRequest(): HttpRequest
}

class ElasticHttpMigration(
        override val version: Int,
        override val applicationId: String,
        private val filePath: String
) : ElasticMigration {

    private val jsonDeserializer = ObjectMapper().registerKotlinModule()

    private data class HttpMigrationDescription(
            val method: String,
            val url: String,
            val data: JsonNode
    )

    override fun getRequest(): HttpUriRequest {

        val migrationDescriptionAsJson = CqlFileBasedMigration::class.java.getResourceAsStream(filePath)
        val migration = jsonDeserializer.readValue(migrationDescriptionAsJson, HttpMigrationDescription::class.java)

        return RequestBuilder.create(migration.method)
                .setUri(URI.create(migration.url))
                .setEntity(ByteArrayEntity(migration.data.toString().toByteArray()))
                .build()
    }
}