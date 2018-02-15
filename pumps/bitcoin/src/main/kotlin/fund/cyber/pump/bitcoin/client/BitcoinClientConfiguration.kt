package fund.cyber.pump.bitcoin.client

import fund.cyber.pump.common.ConcurrentPulledBlockchain
import fund.cyber.pump.common.FlowableBlockchainInterface
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class BitcoinClientConfiguration {

    private val defaultHttpHeaders = listOf(BasicHeader("Keep-Alive", "timeout=10, max=1024"))
    private val connectionManager = PoolingHttpClientConnectionManager().apply {
        defaultMaxPerRoute = 16
        maxTotal = 32
    }

    @Bean
    fun httpClient() = HttpClients.custom()
            .setConnectionManager(connectionManager)
            .setConnectionManagerShared(true)
            .setDefaultHeaders(defaultHttpHeaders)
            .build()!!


    @Bean
    fun blockchainInterface(
            rpcClient: BitcoinJsonRpcClient, rpcToBundleEntitiesConverter: JsonRpcBlockToBitcoinBundleConverter
    ): FlowableBlockchainInterface<BitcoinBlockBundle> {

        val bitcoinBlockchainInterface = BitcoinBlockchainInterface(rpcClient, rpcToBundleEntitiesConverter)
        return ConcurrentPulledBlockchain(bitcoinBlockchainInterface)
    }
}