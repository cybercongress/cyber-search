package fund.cyber.search.model.ethereum

import java.math.BigDecimal

/**
 * Contains full transaction trace tree
 */
data class TxTrace(
    val txHash: String,
    val rootOperationTrace: OperationTrace
)

data class OperationTrace(
    val operation: Operation,
    val result: OperationResult,
    val subtraces: List<OperationTrace>
)

interface Operation

data class CallOperation(
    val type: String,
    val from: String,
    val to: String,
    val input: String,
    val value: BigDecimal,
    val gas: Long
) : Operation

data class CreateContractOperation(
    val from: String,
    val init: String,
    val value: BigDecimal,
    val gas: Long
) : Operation


data class DestroyContractOperation(
    val address: String,
    val balance: BigDecimal,
    val refundAddress: String
) : Operation

data class OperationResult(
    // https://etherscan.io/tx/0x339eb2072aa91861a468e7799b4bff1c36b24c4d2d24bf52988fd52cd1cc4795
    val error: String?, //todo test field
    val address: String?, //todo wtf field?
    val code: String?,//todo wtf field?
    val gasUsed: Long?,
    val output: String?  //raw hex value
)
