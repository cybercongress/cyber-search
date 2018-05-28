package fund.cyber.pump.ethereum.client

import fund.cyber.search.model.chains.ChainInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.web3j.protocol.http.HttpService
import org.web3j.protocol.parity.Parity


@Configuration
class EthereumClientConfiguration {

    @Autowired
    private lateinit var chainInfo: ChainInfo

    @Bean
    fun parityClient() = Parity.build(HttpService(chainInfo.nodeUrl))!!
}
