package fund.cyber.search.model.ethereum

import fund.cyber.search.model.chains.EthereumFamilyChain
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

data class EthereumBlock(
        val number: Long,                   //parsed from hex
        val hash: String,
        val parentHash: String,
        val timestamp: Instant,
        val sha3Uncles: String,
        val logsBloom: String,
        val transactionsRoot: String,
        val stateRoot: String,
        val receiptsRoot: String,
        val miner: String,
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
)

//todo: add properly support of new classic fork


fun getBlockReward(chain: EthereumFamilyChain, number: Long): BigDecimal {
    return if (chain == EthereumFamilyChain.ETHEREUM_CLASSIC) {
        if (number < 5000000) BigDecimal("5") else BigDecimal("4")
    } else {
        if (number < 4370000) BigDecimal("5") else BigDecimal("3")
    }
}