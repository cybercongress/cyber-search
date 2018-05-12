@file:Suppress("MemberVisibilityCanBePrivate")

package fund.cyber.cassandra.ethereum.model

import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.OperationType
import fund.cyber.search.model.ethereum.TxTrace
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant

enum class ExecutionStatus {
    OK, FAILED
}

//todo add tx status
@Table("tx")
data class CqlEthereumTx(
    @PrimaryKey val hash: String,
    val nonce: Long,
    @Column("block_hash") val blockHash: String?,
    @Column("block_number") val blockNumber: Long,
    @Column("first_seen_time") val firstSeenTime: Instant,
    @Column("block_time") val blockTime: Instant?,
    @Column(forceQuote = true) val from: String,
    @Column(forceQuote = true) val to: String?,
    val value: String,
    @Column("gas_price") val gasPrice: BigDecimal,
    @Column("gas_limit") val gasLimit: Long,
    @Column("gas_used") val gasUsed: Long,
    val fee: String,
    val input: String,
    @Column("created_contract") val createdContract: String?,
    @Column("trace_json") val trace: TxTrace?,   //saved in cassandra as json string //todo save as bytes
    val operations: List<CqlTxOperation>
) : CqlEthereumItem {

    constructor(tx: EthereumTx) : this(
        hash = tx.hash, nonce = tx.nonce, blockHash = tx.blockHash, blockNumber = tx.blockNumber,
        blockTime = tx.blockTime, from = tx.from, to = tx.to, firstSeenTime = tx.firstSeenTime,
        value = tx.value.toString(), gasPrice = tx.gasPrice, gasLimit = tx.gasLimit, gasUsed = tx.gasUsed,
        fee = tx.fee.toString(), input = tx.input, createdContract = tx.createdSmartContract, trace = tx.trace,
        operations = emptyList() //todo
    )

    fun contractsUsedInTransaction() = listOfNotNull(from, to, createdContract)
}

data class CqlTxOperation(
    val type: OperationType,
    val from: String,
    val to: String,
    val value: BigDecimal,
    val status: ExecutionStatus, //todo add assumption check for error out of gas for blocks > 0
    val error: String?, //not null if status is FAILED
    val gasUsed: Long,
    val gasLimit: Long
)
