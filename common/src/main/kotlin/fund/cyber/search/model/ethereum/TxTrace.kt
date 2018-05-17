package fund.cyber.search.model.ethereum

/**
 * Contains full transaction trace tree.
 */
data class TxTrace(
    val rootOperationTrace: OperationTrace
) {

    fun getAllOperationsTraces() = allSuboperations(listOf(rootOperationTrace))

    private fun allSuboperations(operations: List<OperationTrace>): List<OperationTrace> {

        if (operations.isEmpty()) return emptyList()

        return operations + operations.flatMap { operation ->
            return@flatMap allSuboperations(operation.subtraces)
        }
    }
}
