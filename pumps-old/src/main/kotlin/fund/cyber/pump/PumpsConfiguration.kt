@file:Suppress("MemberVisibilityCanPrivate", "unused")

package fund.cyber.pump

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import fund.cyber.cassandra.CassandraService
import fund.cyber.node.common.*
import fund.cyber.pump.cassandra.ElassandraStorage
import fund.cyber.pump.kafka.KafkaStorage
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.ehcache.CacheManager
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.xml.XmlConfiguration
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger(PumpsContext::class.java)!!

const val STACK_CACHE_SIZE = "STACK_CACHE_SIZE"
const val STACK_CACHE_SIZE_DEFAULT = 20

object PumpsContext {

    private val defaultHttpHeaders = listOf(BasicHeader("Keep-Alive", "timeout=10, max=1024"))
    private val connectionManager = PoolingHttpClientConnectionManager().apply {
        defaultMaxPerRoute = 16
        maxTotal = 32
    }

    val httpClient = HttpClients.custom()
            .setConnectionManager(connectionManager)
            .setConnectionManagerShared(true)
            .setDefaultHeaders(defaultHttpHeaders)
            .build()!!

    val cassandraService = CassandraService(PumpsConfiguration.cassandraServers, PumpsConfiguration.cassandraPort)

    val elassandraStorage = ElassandraStorage(
            cassandraService = cassandraService, httpClient = httpClient,
            elasticHost = PumpsConfiguration.cassandraServers.first(),
            elasticHttpPort = PumpsConfiguration.elasticHttpPort
    )

    val kafkaStorage by lazy { KafkaStorage() }

    val jacksonJsonSerializer = ObjectMapper().registerKotlinModule().apply {
        this.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
    }

    val jacksonJsonDeserializer = ObjectMapper().registerKotlinModule()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)!!


    val cacheManager by lazy { getCacheManager() }


    fun closeContext() {
        log.info("Closing application context")
        cassandraService.close()
        httpClient.close()
        log.info("Application context is closed")
    }

    val stackCacheSize = env(STACK_CACHE_SIZE, STACK_CACHE_SIZE_DEFAULT)
}


fun getCacheManager(): CacheManager {
    val ehcacheSettingsUri = PumpsContext::class.java.getResource("/ehcache.xml")
    val cacheManager = CacheManagerBuilder.newCacheManager(XmlConfiguration(ehcacheSettingsUri))
    cacheManager.init()
    return cacheManager
}


const val CS_START_BLOCK_DEFAULT = -1L

object PumpsConfiguration {

    val cassandraServers: List<String> = env(CASSANDRA_HOSTS, CASSANDRA_HOSTS_DEFAULT).split(",")
    val cassandraPort: Int = env(CASSANDRA_PORT, CASSANDRA_PORT_DEFAULT)
    val elasticHttpPort: Int = env(ELASTIC_HTTP_PORT, ELASTIC_HTTP_PORT_DEFAULT)

    val kafkaBrokers: List<String> = env(KAFKA_BROKERS, KAFKA_BROKERS_DEFAULT).split(",")

    val chainsToPump: List<Chain> = env("CS_CHAINS_TO_PUMP", "")
            .split(",").map(String::trim).filter(String::isNotEmpty).map(Chain::valueOf)

    val startBlock: Long = env("CS_START_BLOCK", CS_START_BLOCK_DEFAULT)
    val emitsEvents: Boolean = env("CS_EMITS_EVENTS", false)
}