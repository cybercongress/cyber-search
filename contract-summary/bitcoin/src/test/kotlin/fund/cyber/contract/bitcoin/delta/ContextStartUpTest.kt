package fund.cyber.contract.bitcoin.delta

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import fund.cyber.UpdateBitcoinContractSummaryApplication
import fund.cyber.cassandra.CassandraTestBase
import fund.cyber.common.kafka.BaseKafkaIntegrationTestWithStartedKafka
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
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
class BaseBitcoinContextTestConfig {

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
@ContextConfiguration(classes = [UpdateBitcoinContractSummaryApplication::class, BaseBitcoinContextTestConfig::class])
abstract class BaseBitcoinContextTest: BaseKafkaIntegrationTestWithStartedKafka()

@TestPropertySource(properties = ["CHAIN_FAMILY:BITCOIN"])
class ContextStartUpTest: BaseBitcoinContextTest() {

    @Autowired
    lateinit var chainInfo: ChainInfo

    @Autowired
    lateinit var txListener: ConcurrentMessageListenerContainer<PumpEvent, BitcoinTx>

    @Test
    fun shouldCreateContextWithListeners() {
        Assertions.assertThat(chainInfo).isNotNull()
        Assertions.assertThat(chainInfo.family).isEqualTo(ChainFamily.BITCOIN)
        Assertions.assertThat(txListener).isNotNull()
    }
}