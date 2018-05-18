package fund.cyber.cassandra.ethereum.model

import fund.cyber.search.model.ethereum.OperationType
import java.math.BigDecimal
import java.time.Instant

/**
 * Represent operations, moving eth, for contract.
 */
data class CqlEthereumContractOperation(
    val txHash: String,
    val txTime: Instant,
    val contract: String,
    val type: OperationType,
    val from: String,
    val to: String,
    val value: BigDecimal,
    val error: String?, //not null if operation execution failed
    val gasUsed: Long,
    val gasLimit: Long
)
