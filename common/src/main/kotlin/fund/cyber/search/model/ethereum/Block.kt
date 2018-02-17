package fund.cyber.search.model.ethereum

import fund.cyber.search.model.chains.EthereumFamilyChain
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

data class EthereumBlock(
        val number: Long,                   //parsed from hex
        val hash: String,
        val parent_hash: String,
        val timestamp: Instant,
        val sha3_uncles: String,
        val logs_bloom: String,
        val transactions_root: String,
        val state_root: String,
        val receipts_root: String,
        val miner: String,
        val difficulty: BigInteger,
        val total_difficulty: BigInteger,   //parsed from hex
        val extra_data: String,
        val size: Long,                     //parsed from hex
        val gas_limit: Long,                //parsed from hex
        val gas_used: Long,                //parsed from hex
        val tx_number: Int,
        val uncles: List<String>,
        val block_reward: BigDecimal,
        val uncles_reward: BigDecimal,
        val tx_fees: BigDecimal
)

//todo: add properly support of new classic fork


fun getBlockReward(chain: EthereumFamilyChain, number: Long): BigDecimal {
    return if (chain == EthereumFamilyChain.ETHEREUM_CLASSIC) {
        if (number < 5000000) BigDecimal("5") else BigDecimal("4")
    } else {
        if (number < 4370000) BigDecimal("5") else BigDecimal("3")
    }
}