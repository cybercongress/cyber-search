package fund.cyber.search.model.ethereum


/**
 * Represent result of operation invocation.
 */
data class OperationResult(
    val error: String?, //null if operation succeeded
    val address: String?, //todo wtf field?
    val code: String?,//todo wtf field?
    val gasUsed: Long?,
    val output: String?  //raw hex value
)
