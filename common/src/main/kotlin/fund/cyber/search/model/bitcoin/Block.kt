package fund.cyber.search.model.bitcoin

import fund.cyber.search.model.chains.BlockEntity
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant
import kotlin.math.pow


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

fun getBlockReward(height: Long): BigDecimal {
    val power = (height / 210000).toInt()
    return BigDecimal(50 * ((0.5).pow(power)))
}
