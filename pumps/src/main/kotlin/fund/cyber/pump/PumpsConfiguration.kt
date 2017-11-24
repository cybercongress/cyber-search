@file:Suppress("MemberVisibilityCanPrivate")

package fund.cyber.pump

import com.datastax.driver.core.Cluster
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import fund.cyber.dao.migration.ElassandraSchemaMigrationEngine
import fund.cyber.dao.pump.PumpsDaoService
import fund.cyber.dao.system.SystemDaoService
import fund.cyber.node.common.*
import org.apache.http.impl.client.HttpClients
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger(PumpContext::class.java)!!

open class PumpContext<out C : PumpConfiguration>(val configuration: C) {

    val cassandra = Cluster.builder()
            .addContactPoints(*configuration.cassandraServers.toTypedArray())
            .withPort(configuration.cassandraPort)
            .withMaxSchemaAgreementWaitSeconds(30)
            .build().init()!!

    val jacksonJsonSerializer = ObjectMapper().registerKotlinModule()
    val jacksonJsonDeserializer = ObjectMapper().registerKotlinModule()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)!!

    val httpClient = HttpClients.createDefault()!!

    val systemDaoService = SystemDaoService(cassandra)
    val pumpDaoService = PumpsDaoService(cassandra)

    val schemaMigrationEngine = ElassandraSchemaMigrationEngine(
            cassandra = cassandra, httpClient = httpClient, systemDaoService = systemDaoService,
            elasticHost = configuration.cassandraServers.first(), elasticPort = configuration.elasticHttpPort,
            defaultMigrations = PumpsMigrations.migrations
    )

    fun closeContext() {
        log.info("Closing application context")
        cassandra.closeAsync()
        httpClient.close()
        log.info("Application context is closed")
    }
}


const val CS_START_BLOCK_DEFAULT = -1L

open class PumpConfiguration {

    val cassandraServers: List<String> = env(CASSANDRA_HOSTS, CASSANDRA_HOSTS_DEFAULT).split(",")
    val cassandraPort: Int = env(CASSANDRA_PORT, CASSANDRA_PORT_DEFAULT)
    val elasticHttpPort: Int = env(ELASTIC_HTTP_PORT, ELASTIC_HTTP_PORT_DEFAULT)
    val startBlock: Long = env("CS_START_BLOCK", CS_START_BLOCK_DEFAULT)
    val emitsEvents: Boolean = env("CS_EMITS_EVENTS", false)
}