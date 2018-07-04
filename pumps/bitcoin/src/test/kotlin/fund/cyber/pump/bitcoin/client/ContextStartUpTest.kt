package fund.cyber.pump.bitcoin.client

import com.nhaarman.mockito_kotlin.mock
import fund.cyber.BitcoinPumpApplication
import fund.cyber.common.kafka.BaseKafkaIntegrationTestWithStartedKafka
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource


@Configuration
class TestBitcoinPumpContext {

    // mocked rpc client for LastNetworkBlockNumberMonitoring
    @Bean
    fun bitcoinJsonRpcClient() = mock<BitcoinJsonRpcClient> {
        on { getLastBlockNumber() }.thenReturn(100)
    }

}

@DirtiesContext
@ContextConfiguration(classes = [BitcoinPumpApplication::class, TestBitcoinPumpContext::class])
@TestPropertySource(properties = ["CHAIN_FAMILY:BITCOIN"])
class ContextStartUpTest: BaseKafkaIntegrationTestWithStartedKafka() {

    @Autowired
    private lateinit var blockchainInterface: BitcoinBlockchainInterface

    @Test
    fun startUpTest() {
        Assertions.assertThat(blockchainInterface).isNotNull()
    }

}
