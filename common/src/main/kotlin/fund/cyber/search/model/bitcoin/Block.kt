package fund.cyber.search.model.bitcoin

import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant


interface BitcoinItem

data class BitcoinTxPreviewIO(
        val addresses: List<String>,
        val amount: BigDecimal
)

data class BitcoinBlockTx(
        val blockNumber: Long,
        val index: Int,         //specify tx number(order) in block
        val hash: String,
        val fee: BigDecimal,
        val ins: List<BitcoinTxPreviewIO>,
        val outs: List<BitcoinTxPreviewIO>
) : BitcoinItem


data class BitcoinBlock(
        val height: Long,
        val hash: String,
        val miner: String,
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
        val totalOutputsAmount: BigDecimal
) : BitcoinItem

const val BITCOIN_REWARD_FIRST_CHANGE_BLOCK_NUMBER = 210000
const val BITCOIN_REWARD_SECOND_CHANGE_BLOCK_NUMBER = 420000

fun getBlockReward(number: Long): BigDecimal {
    return when {
        number < BITCOIN_REWARD_FIRST_CHANGE_BLOCK_NUMBER -> BigDecimal("50")
        number < BITCOIN_REWARD_SECOND_CHANGE_BLOCK_NUMBER -> BigDecimal("25")
        else -> BigDecimal("12.5")
    }
}
