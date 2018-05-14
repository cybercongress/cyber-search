package fund.cyber.search.model.ethereum

import fund.cyber.common.isZero

/**
 * Contains full transaction trace tree.
 */
data class TxTrace(
    val rootOperationTrace: OperationTrace
) {

    fun getAllOperationsSendingEth(): List<OperationTrace> {
        return getAllOperationsTraces().filter { operationTrace ->
            val operation = operationTrace.operation
            return@filter when (operation) {
                is CallOperation -> !operation.value.isZero()
                is CreateContractOperation -> !operation.value.isZero()
                is DestroyContractOperation -> true
                is RewardOperation -> true
            }
        }
    }

    fun getAllOperationsTraces(): List<OperationTrace> {
        return allSuboperations(listOf(rootOperationTrace))
    }

    private fun allSuboperations(operations: List<OperationTrace>): List<OperationTrace> {

        if (operations.isEmpty()) return emptyList()

        return operations + operations.flatMap { operation ->
            return@flatMap allSuboperations(operation.subtraces)
        }
    }
}
