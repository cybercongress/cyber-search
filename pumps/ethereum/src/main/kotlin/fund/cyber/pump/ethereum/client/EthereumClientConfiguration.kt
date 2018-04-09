package fund.cyber.pump.ethereum.client

import fund.cyber.search.configuration.CHAIN_NODE_URL
import fund.cyber.search.configuration.ETHEREUM_CHAIN_NODE_DEFAULT_URL
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import fund.cyber.search.model.events.unclePumpTopic
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService

const val MAX_PER_ROUTE = 16
const val MAX_TOTAL = 32

@Configuration
class EthereumClientConfiguration {

    @Autowired
    private lateinit var chain: EthereumFamilyChain

    private val defaultHttpHeaders = listOf(BasicHeader("Keep-Alive", "timeout=10, max=1024"))
    private val connectionManager = PoolingHttpClientConnectionManager().apply {
        defaultMaxPerRoute = MAX_PER_ROUTE
        maxTotal = MAX_TOTAL
    }

    private val endpointUrl = env(CHAIN_NODE_URL, ETHEREUM_CHAIN_NODE_DEFAULT_URL)

    @Bean
    fun httpClient() = HttpClients.custom()
            .setConnectionManager(connectionManager)
            .setConnectionManagerShared(true)
            .setDefaultHeaders(defaultHttpHeaders)
            .build()!!

    @Bean
    fun parityClient() = Web3j.build(HttpService(endpointUrl, httpClient()))!!

    @Bean
    fun kafkaTopicNames(): List<String> = listOf(chain.txPumpTopic, chain.blockPumpTopic, chain.unclePumpTopic)
}
