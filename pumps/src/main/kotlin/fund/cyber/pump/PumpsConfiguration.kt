@file:Suppress("MemberVisibilityCanPrivate", "unused")

package fund.cyber.pump

import com.datastax.driver.core.Cluster
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import fund.cyber.dao.migration.ElassandraSchemaMigrationEngine
import fund.cyber.dao.pump.PumpsDaoService
import fund.cyber.dao.system.SystemDaoService
import fund.cyber.node.common.*
import org.apache.http.impl.client.HttpClients
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger(PumpsContext::class.java)!!

object PumpsContext {

    val cassandra = Cluster.builder()
            .addContactPoints(*PumpsConfiguration.cassandraServers.toTypedArray())
            .withPort(PumpsConfiguration.cassandraPort)
            .withMaxSchemaAgreementWaitSeconds(30)
            .build().init()!!

    val jacksonJsonSerializer = ObjectMapper().registerKotlinModule().apply {
        this.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
    }

    val jacksonJsonDeserializer = ObjectMapper().registerKotlinModule()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)!!

    val httpClient = HttpClients.createDefault()!!

    val systemDaoService by lazy { SystemDaoService(cassandra) }
    val pumpDaoService by lazy { PumpsDaoService(cassandra) }

    val schemaMigrationEngine = ElassandraSchemaMigrationEngine(
            cassandra = cassandra, httpClient = httpClient, systemDaoService = systemDaoService,
            elasticHost = PumpsConfiguration.cassandraServers.first(), elasticPort = PumpsConfiguration.elasticHttpPort,
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

object PumpsConfiguration {

    val cassandraServers: List<String> = env(CASSANDRA_HOSTS, CASSANDRA_HOSTS_DEFAULT).split(",")
    val cassandraPort: Int = env(CASSANDRA_PORT, CASSANDRA_PORT_DEFAULT)
    val elasticHttpPort: Int = env(ELASTIC_HTTP_PORT, ELASTIC_HTTP_PORT_DEFAULT)

    val chainsToPump: List<Chains> = env("CS_CHAINS_TO_PUMP", "BITCOIN").split(",").map(Chains::valueOf)
    val startBlock: Long = env("CS_START_BLOCK", CS_START_BLOCK_DEFAULT)
    val emitsEvents: Boolean = env("CS_EMITS_EVENTS", false)
}