package fund.cyber.search.configuration

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import fund.cyber.cassandra.CassandraService
import fund.cyber.node.common.*
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import java.net.InetAddress

object AppContext {

    fun getJsonSerializer(): ObjectMapper = ObjectMapper().registerKotlinModule()
            .registerModule(Jdk8Module())
            .registerModule(JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)


    val searchRequestProcessingStatsKafkaProducer by lazy { SearchRequestProcessingStatsKafkaProducer() }

    private val elasticSettings = Settings.builder()
            .put("client.transport.sniff", true)
            .put("cluster.name", SearchApiConfiguration.elasticClusterName)
            .build()!!

    val elasticClient = PreBuiltTransportClient(elasticSettings)
            .addTransportAddress(InetSocketTransportAddress(
                    InetAddress.getByName(SearchApiConfiguration.cassandraServers.first()), SearchApiConfiguration.elasticTransportPort)
            )!!

    val cassandraService by lazy {
        CassandraService(SearchApiConfiguration.cassandraServers, SearchApiConfiguration.cassandraPort)
    }

    fun closeContext() {
        cassandraService.close()
        elasticClient.close()
    }
}


object SearchApiConfiguration {

    val cassandraServers: List<String> = env(CASSANDRA_HOSTS, CASSANDRA_HOSTS_DEFAULT).split(",")
    val cassandraPort: Int = env(CASSANDRA_PORT, CASSANDRA_PORT_DEFAULT)

    val elasticTransportPort: Int = env(ELASTIC_TRANSPORT_PORT, ELASTIC_TRANSPORT_PORT_DEFAULT)
    val elasticClusterName: String = env(ELASTIC_CLUSTER_NAME, ELASTIC_CLUSTER_NAME_DEFAULT)

    val kafkaBrokers: String = env(KAFKA_BROKERS, KAFKA_BROKERS_DEFAULT)

    val allowedCORS: String = env(CORS_ALLOWED_ORIGINS, CORS_ALLOWED_ORIGINS_DEFAULT)
}