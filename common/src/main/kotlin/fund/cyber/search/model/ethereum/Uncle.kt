package fund.cyber.search.model.ethereum

import fund.cyber.common.decimal32
import fund.cyber.common.decimal8
import fund.cyber.search.model.chains.EthereumFamilyChain
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant

data class EthereumUncle(
        val hash: String,
        val position: Int,
        val number: Long,
        val timestamp: Instant,
        val blockNumber: Long,
        val blockTime: Instant,
        val blockHash: String,
        val miner: String,
        val uncleReward: BigDecimal
)

fun getUncleReward(chain: EthereumFamilyChain, uncleNumber: Long, blockNumber: Long): BigDecimal {

    val blockReward = getBlockReward(chain, blockNumber)
    return if (chain == EthereumFamilyChain.ETHEREUM_CLASSIC) {
        getBlockReward(chain, blockNumber).divide(decimal32, 18, RoundingMode.FLOOR).stripTrailingZeros()
    } else {
        ((uncleNumber.toBigDecimal() + decimal8 - blockNumber.toBigDecimal()) * blockReward)
                .divide(decimal8, 18, RoundingMode.FLOOR).stripTrailingZeros()
    }
}