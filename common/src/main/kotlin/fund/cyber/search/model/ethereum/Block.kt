package fund.cyber.search.model.ethereum

import fund.cyber.search.model.chains.BlockEntity
import fund.cyber.search.model.chains.ChainInfo
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

const val ETHEREUM_CLASSIC_REWARD_CHANGED_BLOCK_NUMBER = 5000000
const val ETHEREUM_REWARD_CHANGED_BLOCK_NUMBER = 4370000

data class EthereumBlock(
        override val number: Long,                   //parsed from hex
        val hash: String,
        val parentHash: String,
        val timestamp: Instant,
        val sha3Uncles: String,
        val logsBloom: String,
        val transactionsRoot: String,
        val stateRoot: String,
        val receiptsRoot: String,
        val minerContractHash: String,
        val nonce: Long,                    //parsed from hex
        val difficulty: BigInteger,
        val totalDifficulty: BigInteger,   //parsed from hex
        val extraData: String,
        val size: Long,                     //parsed from hex
        val gasLimit: Long,                //parsed from hex
        val gasUsed: Long,                //parsed from hex
        val txNumber: Int,
        val uncles: List<String>,
        val blockReward: BigDecimal,
        val unclesReward: BigDecimal,
        val txFees: BigDecimal
) : BlockEntity

//todo: 1) add properly support of new classic fork. 2) add support of custom reward functions in forks
fun getBlockReward(chainInfo: ChainInfo, number: Long): BigDecimal {
    return if (chainInfo.fullName == "ETHEREUM_CLASSIC") {
        if (number < ETHEREUM_CLASSIC_REWARD_CHANGED_BLOCK_NUMBER) BigDecimal("5") else BigDecimal("4")
    } else {
        if (number < ETHEREUM_REWARD_CHANGED_BLOCK_NUMBER) BigDecimal("5") else BigDecimal("3")
    }
}
