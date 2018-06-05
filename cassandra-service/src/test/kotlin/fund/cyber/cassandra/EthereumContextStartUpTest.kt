package fund.cyber.cassandra

import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.search.model.chains.ChainFamily.ETHEREUM
import fund.cyber.search.model.chains.ChainInfo
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource


@Configuration
class EthereumContext {

    @Bean
    fun chainInfo() = ChainInfo(ETHEREUM)
}

@Disabled
@TestPropertySource(properties = ["CHAIN_FAMILY:ETHEREUM"])
@ContextConfiguration(classes = [EthereumContext::class])
class EthereumContextStartUpTest : CassandraTestBase() {

    @Autowired
    lateinit var blockRepository: EthereumBlockRepository

    @Test
    @DisplayName("Should successfully create context for Ethereum repositories")
    fun shouldUseTransactionMessageListenerContainer() {
        Assertions.assertTrue(true)
    }
}