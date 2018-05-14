package fund.cyber.search.model.ethereum


/**
 * Represent result of operation invocation.
 * For Reward and Destroy, result field always null.
 */
interface OperationResult

data class ErroredOperationResult(
    val error: String
) : OperationResult

data class CallOperationResult(
    val gasUsed: Long,
    val output: String  //raw hex value
) : OperationResult

data class CreateContractOperationResult(
    val address: String, // created contract address
    val code: String, // created contract bytecode
    val gasUsed: Long
) : OperationResult

