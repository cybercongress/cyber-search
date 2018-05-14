package fund.cyber.search.model.ethereum

import fund.cyber.search.model.PoolItem
import java.math.BigDecimal
import java.time.Instant

val weiToEthRate = BigDecimal("1E-18")

data class EthereumTx(
    val hash: String,
    val error: String?,
    val nonce: Long,
    val blockHash: String?,                      // null when tx is pending (in memory pool)
    val blockNumber: Long,                       // null when tx is pending (in memory pool)
    val firstSeenTime: Instant,
    val blockTime: Instant?,
    val positionInBlock: Int,                    // txes from one block ordering field
    val from: String,
    val to: String?,                             // null when its a contract creation transaction.
    val value: BigDecimal,
    val gasPrice: BigDecimal,
    val gasLimit: Long,
    val gasUsed: Long,
    val fee: BigDecimal,                         // calculated

    val input: String,                           // tx payload, used to specify invoking method with arguments,
                                                 // or bytecode for smart contract creation

    val createdSmartContract: String?,           // not null if tx creates smart contract
    val trace: TxTrace?                          // null only for pending txes (in memory pool)
) : PoolItem {

    fun contractsUsedInTransaction() = listOfNotNull(from, to, createdSmartContract).filter(String::isNotEmpty)

    fun mempoolState() = EthereumTx(
        hash = this.hash, nonce = this.nonce, blockHash = null, blockNumber = -1, error = this.error,
        blockTime = null, positionInBlock = -1, from = this.from, to = this.to,
        value = this.value, gasPrice = this.gasPrice, gasLimit = this.gasLimit,
        gasUsed = 0, fee = this.gasPrice * this.gasLimit.toBigDecimal(), input = this.input,
        createdSmartContract = this.createdSmartContract, firstSeenTime = this.firstSeenTime, trace = trace
    )
}
