package fund.cyber.contract.ethereum

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import fund.cyber.UpdateEthereumContractSummaryApplication
import fund.cyber.cassandra.CassandraTestBase
import fund.cyber.common.kafka.BaseKafkaIntegrationTestWithStartedKafka
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import org.apache.http.HttpResponse
import org.apache.http.HttpStatus
import org.apache.http.StatusLine
import org.apache.http.client.HttpClient
import org.apache.http.message.BasicHttpResponse
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource

@Configuration
class BaseEthereumContextTestConfig {

    @Bean
    @Primary
    fun mockedHttpClient() = mock<HttpClient> {
        on { execute(any()) } doReturn httpResponseOk()
    }

    private fun httpResponseOk(): HttpResponse {
        val statusLineOk = object : StatusLine {
            override fun getStatusCode() = HttpStatus.SC_OK
            override fun getProtocolVersion() = null
            override fun getReasonPhrase() = null
        }
        return BasicHttpResponse(statusLineOk)
    }
}

@CassandraTestBase
@DirtiesContext
@ContextConfiguration(classes = [UpdateEthereumContractSummaryApplication::class, BaseEthereumContextTestConfig::class])
abstract class BaseEthereumContextTest: BaseKafkaIntegrationTestWithStartedKafka()

@TestPropertySource(properties = ["CHAIN_FAMILY:ETHEREUM"])
class ContextStartUpTest: BaseEthereumContextTest() {

    @Autowired
    lateinit var chainInfo: ChainInfo

    @Autowired
    lateinit var txListener: ConcurrentMessageListenerContainer<PumpEvent, EthereumTx>

    @Autowired
    lateinit var blockListener: ConcurrentMessageListenerContainer<PumpEvent, EthereumBlock>

    @Autowired
    lateinit var uncleListener: ConcurrentMessageListenerContainer<PumpEvent, EthereumUncle>

    @Test
    fun shouldCreateContextWithListeners() {
        Assertions.assertThat(chainInfo).isNotNull()
        Assertions.assertThat(chainInfo.family).isEqualTo(ChainFamily.ETHEREUM)
        Assertions.assertThat(txListener).isNotNull()
        Assertions.assertThat(blockListener).isNotNull()
        Assertions.assertThat(uncleListener).isNotNull()
    }
}