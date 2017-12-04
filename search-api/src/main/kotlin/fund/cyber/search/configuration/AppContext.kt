package fund.cyber.search.configuration

import com.datastax.driver.core.Cluster
import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.dao.ethereum.EthereumDaoService
import fund.cyber.node.common.*
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import java.net.InetAddress

object AppContext {

    val concurrentContext by lazy { newFixedThreadPoolContext(4, "Coroutines Concurrent Pool") }

    val jsonSerializer = ObjectMapper()
    val jsonDeserializer = ObjectMapper()

    val searchRequestProcessingStatsKafkaProducer by lazy { SearchRequestProcessingStatsKafkaProducer() }

    private val elasticSettings = Settings.builder()
            .put("client.transport.sniff", true)
            .put("cluster.name", SearchApiConfiguration.elasticClusterName)
            .build()!!

    val elasticClient = PreBuiltTransportClient(elasticSettings)
            .addTransportAddress(InetSocketTransportAddress(
                    InetAddress.getByName(SearchApiConfiguration.cassandraServers.first()), SearchApiConfiguration.elasticTransportPort)
            )!!

    val cassandra = Cluster.builder()
            .addContactPoints(*SearchApiConfiguration.cassandraServers.toTypedArray())
            .withPort(SearchApiConfiguration.cassandraPort)
            .withMaxSchemaAgreementWaitSeconds(30)
            .build().init()!!

    val bitcoinDaoService by lazy { BitcoinDaoService(cassandra) }
    val ethereumDaoService by lazy { EthereumDaoService(cassandra) }


    fun closeContext() {
        cassandra.closeAsync()
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