package fund.cyber.pump.ethereum.client

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import fund.cyber.EthereumPumpApplication
import fund.cyber.common.kafka.BaseKafkaIntegrationTestWithStartedKafka
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import org.web3j.protocol.Web3jService
import org.web3j.protocol.core.methods.response.EthBlockNumber
import org.web3j.protocol.parity.JsonRpc2_0Parity
import org.web3j.protocol.parity.Parity

@Configuration
class TestEthereumPumpContext {

    private val ethBlockNumberMockValue = EthBlockNumber().apply { this.result = "0x64" }

    // mocked parity client for LastNetworkBlockNumberMonitoring
    @Bean
    fun parityClient(): Parity {
        return JsonRpc2_0Parity(web3jServiceMock())
    }

    fun web3jServiceMock() = mock<Web3jService> {
        on { send(any(), eq(EthBlockNumber::class.java)) }.thenReturn(ethBlockNumberMockValue)
    }

}

@ContextConfiguration(classes = [EthereumPumpApplication::class, TestEthereumPumpContext::class])
@TestPropertySource(properties = ["CHAIN_FAMILY:ETHEREUM"])
class ContextStartUpTest: BaseKafkaIntegrationTestWithStartedKafka() {

    @Autowired
    private lateinit var blockchainInterface: EthereumBlockchainInterface

    @Test
    fun startUpTest() {
        Assertions.assertThat(blockchainInterface).isNotNull()
    }

}
