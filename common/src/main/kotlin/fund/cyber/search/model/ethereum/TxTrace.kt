package fund.cyber.search.model.ethereum

import fund.cyber.common.isZero

/**
 * Contains full transaction trace tree.
 */
data class TxTrace(
    val rootOperationTrace: OperationTrace
) {

    fun getAllOperationsSendingEth(): List<OperationTrace> {
        return allSuboperations(listOf(rootOperationTrace)).filter { operationTrace ->
            val operation = operationTrace.operation
            return@filter when (operation) {
                is CallOperation -> !operation.value.isZero()
                is CreateContractOperation -> !operation.value.isZero()
                is DestroyContractOperation -> true
                is RewardOperation -> true
            }
        }
    }

    private fun allSuboperations(operations: List<OperationTrace>): List<OperationTrace> {
        return operations.flatMap { operation ->
            if (operation.subtraces.isEmpty()) {
                return@flatMap listOf(operation)
            }
            return@flatMap allSuboperations(operation.subtraces)
        }
    }
}
