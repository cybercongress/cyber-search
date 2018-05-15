package fund.cyber.search.model.ethereum

import fund.cyber.common.decimal32
import fund.cyber.common.decimal8
import fund.cyber.search.model.chains.ChainEntity
import fund.cyber.search.model.chains.ChainInfo
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant

const val DIVIDE_SCALE = 18

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
) : ChainEntity

//todo: add support of custom reward functions in forks
fun getUncleReward(chainInfo: ChainInfo, uncleNumber: Long, blockNumber: Long): BigDecimal {

    val blockReward = getBlockReward(chainInfo, blockNumber)
    return if (chainInfo.fullName == "ETHEREUM_CLASSIC") {
        getBlockReward(chainInfo, blockNumber).divide(decimal32, DIVIDE_SCALE, RoundingMode.FLOOR).stripTrailingZeros()
    } else {
        ((uncleNumber.toBigDecimal() + decimal8 - blockNumber.toBigDecimal()) * blockReward)
                .divide(decimal8, DIVIDE_SCALE, RoundingMode.FLOOR).stripTrailingZeros()
    }
}
