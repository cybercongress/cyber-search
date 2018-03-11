package fund.cyber.pump.ethereum.client

import fund.cyber.search.configuration.CHAIN_NODE_URL
import fund.cyber.search.configuration.ETHEREUM_CHAIN_NODE_DEFAULT_URL
import fund.cyber.search.configuration.env
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService

@Configuration
class EthereumClientConfiguration {

    private val defaultHttpHeaders = listOf(BasicHeader("Keep-Alive", "timeout=10, max=1024"))
    private val connectionManager = PoolingHttpClientConnectionManager().apply {
        defaultMaxPerRoute = 16
        maxTotal = 32
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
}