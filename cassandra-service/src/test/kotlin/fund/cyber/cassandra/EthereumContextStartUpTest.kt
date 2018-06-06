package fund.cyber.cassandra

import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource

@TestPropertySource(properties = ["CHAIN_FAMILY:ETHEREUM"])
class EthereumContextStartUpTest : CassandraTestBase() {

    @Autowired
    lateinit var blockRepository: EthereumBlockRepository

    @Test
    @DisplayName("Should successfully create context for Ethereum repositories")
    fun shouldCreateContextForEthereumChain() {
        Assertions.assertNull(blockRepository.findAll().blockFirst())
    }
}