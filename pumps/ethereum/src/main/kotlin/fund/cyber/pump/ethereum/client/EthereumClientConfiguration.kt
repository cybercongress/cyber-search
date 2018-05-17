package fund.cyber.pump.ethereum.client

import fund.cyber.search.model.chains.ChainInfo
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.web3j.protocol.parity.Parity
import org.web3j.protocol.http.HttpService

const val MAX_PER_ROUTE = 16
const val MAX_TOTAL = 32

@Configuration
class EthereumClientConfiguration {

    @Autowired
    private lateinit var chainInfo: ChainInfo

    private val defaultHttpHeaders = listOf(BasicHeader("Keep-Alive", "timeout=10, max=1024"))
    private val connectionManager = PoolingHttpClientConnectionManager().apply {
        defaultMaxPerRoute = MAX_PER_ROUTE
        maxTotal = MAX_TOTAL
    }

    @Bean
    fun httpClient() = HttpClients.custom()
            .setConnectionManager(connectionManager)
            .setConnectionManagerShared(true)
            .setDefaultHeaders(defaultHttpHeaders)
            .build()!!

    @Bean
    fun parityClient() = Parity.build(HttpService(chainInfo.nodeUrl))!!

}
