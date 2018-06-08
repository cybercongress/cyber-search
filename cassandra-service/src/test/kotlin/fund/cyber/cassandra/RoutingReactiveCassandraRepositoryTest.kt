package fund.cyber.cassandra

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.common.RoutingReactiveCassandraRepository
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

@DirtiesContext
@TestPropertySource(properties = ["CHAIN:BITCOIN"])
class RoutingReactiveCassandraRepositoryTest : CassandraTestBase() {

    @Autowired
    lateinit var blockRepository: BitcoinBlockRepository

    private val testBlock = CqlBitcoinBlock(
        number = 13, hash = "H", txNumber = 158, minerContractHash = "M", difficulty = BigInteger("0"),
        size = 0, blockReward = BigDecimal("0"), txFees = BigDecimal("0"), timestamp = Instant.ofEpochMilli(1000000),
        coinbaseData = "0x", nonce = 0, merkleroot = "0x", version = 0, weight = 0,
        totalOutputsValue = BigDecimal.ZERO.toString(), bits = "0x", parentHash = "PH"
    )

    @Test
    @DisplayName("Should successfully create and delete entities")
    fun shouldCorrectSaveAndDeleteEntities() {

        @Suppress("USELESS_IS_CHECK")
        Assertions.assertTrue(blockRepository is RoutingReactiveCassandraRepository<*, *>)

        blockRepository.save(testBlock).block()
        Assertions.assertNotNull(blockRepository.findById(testBlock.number).block())

        blockRepository.deleteById(testBlock.number).block()
        Assertions.assertNull(blockRepository.findById(testBlock.number).block())

        blockRepository.saveAll(listOf(testBlock)).collectList().block()
        Assertions.assertNotNull(blockRepository.findById(testBlock.number).block())
    }
}