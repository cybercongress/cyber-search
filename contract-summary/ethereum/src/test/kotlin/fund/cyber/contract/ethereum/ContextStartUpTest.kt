package fund.cyber.contract.ethereum

import fund.cyber.UpdateEthereumContractSummaryApplication
import fund.cyber.cassandra.CassandraTestBase
import fund.cyber.common.kafka.BaseKafkaIntegrationTestWithStartedKafka
import fund.cyber.search.model.chains.ChainInfo
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource


@CassandraTestBase
@ContextConfiguration(classes = [UpdateEthereumContractSummaryApplication::class])
abstract class BaseEthereumContextTest: BaseKafkaIntegrationTestWithStartedKafka()

@TestPropertySource(properties = ["CHAIN_FAMILY:ETHEREUM"])
class ContextStartUpTest: BaseEthereumContextTest() {

    @Autowired
    lateinit var chainInfo: ChainInfo

    @Test
    fun startUpTest() {
        Assertions.assertThat(chainInfo).isNotNull()
        println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11")
    }
}