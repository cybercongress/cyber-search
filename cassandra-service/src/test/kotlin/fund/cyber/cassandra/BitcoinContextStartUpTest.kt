package fund.cyber.cassandra

import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource

@DirtiesContext
@TestPropertySource(properties = ["CHAIN_FAMILY:BITCOIN"])
class BitcoinContextStartUpTest : BaseCassandraServiceTest() {

    @Autowired
    lateinit var blockRepository: BitcoinBlockRepository

    @Test
    @DisplayName("Should successfully create context for Bitcoin repositories")
    fun shouldCreateContextForBitcoinChain() {
        Assertions.assertNull(blockRepository.findAll().blockFirst())
    }
}