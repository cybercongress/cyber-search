package fund.cyber.search.model.ethereum

import fund.cyber.search.model.chains.BlockEntity
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

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
    val uncles: List<String> = emptyList(),
    val blockReward: BigDecimal,
    val unclesReward: BigDecimal,          //including uncles reward, todo rename
    val txFees: BigDecimal
) : BlockEntity
