package fund.cyber.search.model.bitcoin

import org.junit.Test
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

@DisplayName("BitcoinBlock tests")
class BlockTest {

    @Test
    @DisplayName("Should correctly calculate bitcoin mining reward given block height")
    fun shouldCorrectlyCalculateBlockReward() {
        Assertions.assertEquals(BigDecimal(50), createBlock(1).blockReward)
        Assertions.assertEquals(BigDecimal(25), createBlock(210000).blockReward)
        Assertions.assertEquals(BigDecimal(12.5), createBlock(420000).blockReward)
        Assertions.assertEquals(BigDecimal(6.25), createBlock(630000).blockReward)
        Assertions.assertEquals(BigDecimal(3.125), createBlock(840000).blockReward)
    }

    private fun createBlock(height: Long) = BitcoinBlock (
        height = height, hash = "H", parentHash = "PH", minerContractHash = "miner1",
        blockReward = getBlockReward(height), txFees = BigDecimal(0), coinbaseData = "test",
        time = Instant.now(), nonce = 1, merkleroot = "1", size = 1, version = 1, weight = 1,
        bits = "1", difficulty = BigInteger("0"), txNumber = 1, totalOutputsAmount = BigDecimal("1")
    )
}