package fund.cyber.contract.ethereum.delta

import fund.cyber.cassandra.ethereum.model.CqlEthereumContractSummary
import fund.cyber.contract.common.delta.DeltaProcessor
import fund.cyber.contract.ethereum.summary.EthereumContractSummaryDelta
import fund.cyber.search.model.ethereum.CallOperation
import fund.cyber.search.model.ethereum.CreateContractOperation
import fund.cyber.search.model.ethereum.CreateContractOperationResult
import fund.cyber.search.model.ethereum.DestroyContractOperation
import fund.cyber.search.model.ethereum.ErroredOperationResult
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.OperationResult
import fund.cyber.search.model.ethereum.OperationTrace
import fund.cyber.search.model.ethereum.RewardOperation
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component
import java.math.BigDecimal.ZERO


@Component
class EthereumTxDeltaProcessor : DeltaProcessor<EthereumTx, CqlEthereumContractSummary, EthereumContractSummaryDelta> {

    override fun recordToDeltas(record: ConsumerRecord<PumpEvent, EthereumTx>): List<EthereumContractSummaryDelta> {

        val event = record.key()
        if (event == PumpEvent.NEW_POOL_TX) return emptyList()

        val trace = record.value().trace!!
        val rootOpsDeltas = deltasByOperationTrace(trace.rootOperationTrace, record, true)

        val deltasByOps = if (trace.rootOperationTrace.result !is ErroredOperationResult) {
            rootOpsDeltas + deltasByOperations(trace.rootOperationTrace.subtraces, record)
        } else {
            rootOpsDeltas
        }

        return deltasByOps.map { delta -> if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta }
    }

    override fun affectedContracts(records: List<ConsumerRecord<PumpEvent, EthereumTx>>): Set<String> {
        val allContracts: List<String> = records.flatMap { record ->
            val inContract = record.value().from
            val outContract = (record.value().to ?: record.value().createdSmartContract)!!
            return@flatMap listOf(inContract, outContract)
        }

        return allContracts.filter { contract -> contract.isNotEmpty() }.toSet()
    }

    private fun deltasByOperations(
        operations: List<OperationTrace>, record: ConsumerRecord<PumpEvent, EthereumTx>
    ): List<EthereumContractSummaryDelta> {

        return operations
            .filter { op -> op.result !is ErroredOperationResult }
            .flatMap { op ->
                val opDeltas = deltasByOperationTrace(op, record)
                return@flatMap opDeltas + deltasByOperations(op.subtraces, record)
            }
    }

    private fun deltasByOperationTrace(
        opTrace: OperationTrace, record: ConsumerRecord<PumpEvent, EthereumTx>, isRootOp: Boolean = false
    ): List<EthereumContractSummaryDelta> {

        val op = opTrace.operation
        val opResult = opTrace.result  // not null for call and create contract ops

        return when (op) {
            is CallOperation -> deltasByCallOperationTrace(op, opResult!!, isRootOp, record)
            is CreateContractOperation -> deltasByCreateOperation(op, opResult!!, record, isRootOp)
            is DestroyContractOperation -> deltasByDestroyOperation(op, record)
            is RewardOperation -> emptyList() //not possible here
        }
    }

    /**
     * If parent operation failed, should no be invoked. Returns delta for destroyed contract and refund contract.
     */
    @SuppressWarnings("ComplexMethod")
    private fun deltasByCallOperationTrace(
        op: CallOperation, opResult: OperationResult, isRootOp: Boolean = false,
        record: ConsumerRecord<PumpEvent, EthereumTx>
    ): List<EthereumContractSummaryDelta> {

        val tx = record.value()
        val fee = if (isRootOp) tx.fee else ZERO // fee applied only for tx -> only counted for root op
        val opFailed = opResult is ErroredOperationResult
        val balanceDelta = -(fee + if (opFailed) ZERO else op.value)

        val contractDeltaByInput = EthereumContractSummaryDelta(
            contract = op.from, lastOpTime = tx.blockTime!!, balanceDelta = balanceDelta,
            successfulOpNumberDelta = if (opFailed) 0 else 1, smartContract = false,
            txNumberDelta = if (isRootOp) 1 else 0, // tx count increased only for root op contracts
            topic = record.topic(), partition = record.partition(), offset = record.offset()
        )

        val contractDeltaByOutput = EthereumContractSummaryDelta(
            contract = op.to, lastOpTime = tx.blockTime!!, smartContract = false,
            totalReceivedDelta = if (opFailed) ZERO else op.value,
            balanceDelta = if (opFailed) ZERO else op.value,
            successfulOpNumberDelta = if (opFailed) 0 else 1,
            txNumberDelta = if (isRootOp) 1 else 0, // tx count increased only for root op contracts
            topic = record.topic(), partition = record.partition(), offset = record.offset()
        )
        return listOf(contractDeltaByInput, contractDeltaByOutput)
    }

    /**
     * If parent operation failed, should no be invoked. Returns delta for destroyed contract and refund contract.
     */
    @SuppressWarnings("ComplexMethod")
    private fun deltasByCreateOperation(
        op: CreateContractOperation, opResult: OperationResult,
        record: ConsumerRecord<PumpEvent, EthereumTx>, rootOperation: Boolean = false
    ): List<EthereumContractSummaryDelta> {

        val tx = record.value()
        val fee = if (rootOperation) tx.fee else ZERO // fee applied only for tx -> only counted for root op
        val opFailed = opResult is ErroredOperationResult
        val balanceDelta = -(fee + if (opFailed) ZERO else op.value)

        val contractDeltaByInput = EthereumContractSummaryDelta(
            contract = op.from, lastOpTime = tx.blockTime!!, balanceDelta = balanceDelta,
            successfulOpNumberDelta = if (rootOperation && opFailed) 0 else 1,
            txNumberDelta = if (rootOperation) 1 else 0, smartContract = false,
            topic = record.topic(), partition = record.partition(), offset = record.offset()
        )

        if (opFailed) return listOf(contractDeltaByInput)

        opResult as CreateContractOperationResult
        val contractDeltaBySmartContract = EthereumContractSummaryDelta(
            contract = opResult.address, lastOpTime = tx.blockTime!!, smartContract = true,
            successfulOpNumberDelta = if (rootOperation && opFailed) 0 else 1,
            totalReceivedDelta = if (opFailed) ZERO else op.value,
            balanceDelta = if (opFailed) ZERO else op.value,
            txNumberDelta = if (rootOperation) 1 else 0,
            topic = record.topic(), partition = record.partition(), offset = record.offset()
        )
        return listOf(contractDeltaByInput, contractDeltaBySmartContract)
    }

    /**
     * If parent operation failed, should no be invoked. Returns delta for destroyed contract and refund contract.
     * Can't be root op.
     */
    private fun deltasByDestroyOperation(
        op: DestroyContractOperation, record: ConsumerRecord<PumpEvent, EthereumTx>
    ): List<EthereumContractSummaryDelta> {

        val tx = record.value()

        val contractDelta = EthereumContractSummaryDelta(
            contract = op.contractToDestroy, lastOpTime = tx.blockTime!!, successfulOpNumberDelta = 1,
            balanceDelta = op.refundValue.negate(), smartContract = false,
            topic = record.topic(), partition = record.partition(), offset = record.offset()
        )

        val refundDelta = EthereumContractSummaryDelta(
            contract = op.refundContract, successfulOpNumberDelta = 1, lastOpTime = tx.blockTime!!,
            totalReceivedDelta = op.refundValue, balanceDelta = op.refundValue, smartContract = false,
            topic = record.topic(), partition = record.partition(), offset = record.offset()
        )
        return listOf(contractDelta, refundDelta)
    }
}
