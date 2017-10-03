package fund.cyber.search.context

import com.datastax.driver.core.Cluster
import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.dao.ethereum.EthereumDaoService
import fund.cyber.search.configuration.SearchApiConfiguration
import fund.cyber.search.configuration.SearchRequestProcessingStatsKafkaProducer
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import java.net.InetAddress

object AppContext {

    val concurrentContext = newFixedThreadPoolContext(4, "Coroutines Concurrent Pool")

    val configuration = SearchApiConfiguration()

    val jsonSerializer = ObjectMapper()
    val jsonDeserializer = ObjectMapper()

    val searchRequestProcessingStatsKafkaProducer = SearchRequestProcessingStatsKafkaProducer(configuration)

    private val elasticSettings = Settings.settingsBuilder().put("cluster.name", configuration.elasticClusterName).build()!!
    val elasticClient = TransportClient.builder().settings(elasticSettings).build()
            .addTransportAddress(InetSocketTransportAddress(
                    InetAddress.getByName(configuration.elasticHost), configuration.elasticPort)
            )!!

    val cassandraClient = Cluster.builder().addContactPoint(configuration.cassandraHost).build().init()!!

    val bitcoinDaoService = BitcoinDaoService(cassandraClient)
    val ethereumDaoService = EthereumDaoService(cassandraClient)
}