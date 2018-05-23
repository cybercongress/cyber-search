package fund.cyber.search.model.ethereum

import com.fasterxml.jackson.annotation.JsonIgnore

/**
 * Contains full transaction trace tree.
 */
data class TxTrace(
    val rootOperationTrace: OperationTrace
) {

    @JsonIgnore
    fun isRootOperationFailed() = rootOperationTrace.result is ErroredOperationResult

    @JsonIgnore
    fun getAllOperationsTraces() = allSuboperations(listOf(rootOperationTrace))

    private fun allSuboperations(operations: List<OperationTrace>): List<OperationTrace> {

        if (operations.isEmpty()) return emptyList()

        return operations + operations.flatMap { operation ->
            return@flatMap allSuboperations(operation.subtraces)
        }
    }
}
