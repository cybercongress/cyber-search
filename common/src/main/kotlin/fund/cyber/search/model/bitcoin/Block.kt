package fund.cyber.search.model.bitcoin

import fund.cyber.search.model.chains.BlockEntity
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant


data class BitcoinBlock(
        val height: Long,
        val hash: String,
        val parentHash: String,
        val minerContractHash: String,
        val blockReward: BigDecimal,
        val txFees: BigDecimal,
        val coinbaseData: String,
        val time: Instant,
        val nonce: Long,
        val merkleroot: String,
        val size: Int,
        val version: Int,
        val weight: Int,
        val bits: String,
        val difficulty: BigInteger,
        val txNumber: Int,
        val totalOutputsAmount: BigDecimal,
        override val number: Long = height
) : BlockEntity

const val BITCOIN_REWARD_FIRST_CHANGE_BLOCK_NUMBER = 210000
const val BITCOIN_REWARD_SECOND_CHANGE_BLOCK_NUMBER = 420000

fun getBlockReward(number: Long): BigDecimal {
    return when {
        number < BITCOIN_REWARD_FIRST_CHANGE_BLOCK_NUMBER -> BigDecimal("50")
        number < BITCOIN_REWARD_SECOND_CHANGE_BLOCK_NUMBER -> BigDecimal("25")
        else -> BigDecimal("12.5")
    }
}
