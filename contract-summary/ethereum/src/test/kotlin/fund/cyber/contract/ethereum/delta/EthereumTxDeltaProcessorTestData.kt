package fund.cyber.contract.ethereum.delta

import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.CallOperation
import fund.cyber.search.model.ethereum.CallOperationResult
import fund.cyber.search.model.ethereum.CreateContractOperation
import fund.cyber.search.model.ethereum.CreateContractOperationResult
import fund.cyber.search.model.ethereum.DestroyContractOperation
import fund.cyber.search.model.ethereum.ErroredOperationResult
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.OperationTrace
import fund.cyber.search.model.ethereum.OperationType
import fund.cyber.search.model.ethereum.OperationType.CALL
import fund.cyber.search.model.ethereum.OperationType.CREATE_CONTRACT
import fund.cyber.search.model.ethereum.OperationType.DESTROY_CONTRACT
import fund.cyber.search.model.ethereum.TxTrace
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.PumpEvent.NEW_BLOCK
import fund.cyber.search.model.events.txPumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal
import java.math.BigDecimal.ZERO
import java.time.Instant


private val time = Instant.ofEpochMilli(100000)
internal const val rootTxFrom = "95bb2b7"
internal const val rootTxTo = "88e61af"

internal const val txFrom = "48f2hl3"
internal const val txTo = "1g42hl3"

private val chainInfo = ChainInfo(ChainFamily.ETHEREUM)

@SuppressWarnings("LongParameterList", "ComplexMethod")
internal fun delta(
    contract: String, balanceDelta: BigDecimal, reverted: Boolean = false,
    txDelta: Boolean = false, opDelta: Boolean = true, smartContract: Boolean = false
) = EthereumContractSummaryDelta(
    contract = contract, smartContract = smartContract,
    balanceDelta = if (reverted) -balanceDelta else balanceDelta,
    totalReceivedDelta = if (reverted && balanceDelta > ZERO) -balanceDelta else if (balanceDelta > ZERO) balanceDelta else ZERO,
    txNumberDelta = if (!txDelta) 0 else if (reverted) -1 else 1,
    successfulOpNumberDelta = if (!opDelta) 0 else if (reverted) -1 else 1,
    topic = chainInfo.txPumpTopic, partition = 0, offset = 0, lastOpTime = time
)


internal fun txRecord(
    value: BigDecimal, event: PumpEvent = NEW_BLOCK, input: String = "0x", error: String? = null, trace: TxTrace? = null
): ConsumerRecord<PumpEvent, EthereumTx> {

    val tx = EthereumTx(
        hash = "0x64c95b3", nonce = 0, blockHash = "0x97bda1",
        firstSeenTime = time, error = error, blockNumber = 4959189, blockTime = time, positionInBlock = 1,
        from = rootTxFrom, to = rootTxTo, value = value, gasPrice = BigDecimal("0.000000023"), gasLimit = 21000L,
        gasUsed = 20000L, fee = BigDecimal("0.00046"), input = input, createdSmartContract = null, trace = trace
    )
    return ConsumerRecord(chainInfo.txPumpTopic, 0, 0, event, tx)
}


internal fun rootOperationTrace(
    operationType: OperationType = CALL,
    value: BigDecimal = ZERO,
    subtraces: List<OperationTrace> = emptyList(),
    error: String? = null
): OperationTrace {
    return when (operationType) {
        CALL -> callOperationTrace(rootTxFrom, rootTxTo, value, subtraces, error)
        CREATE_CONTRACT -> createContractOperationTrace(rootTxFrom, rootTxTo, value, subtraces, error)
        DESTROY_CONTRACT -> throw RuntimeException() //cant be root operation
        else -> throw RuntimeException()
    }
}


internal fun callOperationTrace(
    from: String = txFrom, to: String = txTo, value: BigDecimal = ZERO,
    subtraces: List<OperationTrace> = emptyList(), error: String? = null
) = OperationTrace(
    operation = CallOperation(from = from, to = to, gasLimit = 21000L, type = "call", input = "0x", value = value),
    result = if (error != null) ErroredOperationResult(error) else CallOperationResult(gasUsed = 20000L, output = "0x"),
    subtraces = subtraces
)


internal fun createContractOperationTrace(
    from: String = txFrom, to: String = txTo, value: BigDecimal = ZERO,
    subtraces: List<OperationTrace> = emptyList(), error: String? = null
) = OperationTrace(
    operation = CreateContractOperation(from = from, value = value, gasLimit = 21000L, init = "code"),
    result = if (error != null) ErroredOperationResult(error) else CreateContractOperationResult(
        address = to, code = "code", gasUsed = 20000L
    ),
    subtraces = subtraces
)


internal fun destroyContractOperationTrace(
    contractToDelete: String = rootTxTo, refundContract: String = txTo, value: BigDecimal = ZERO, error: String? = null
) = OperationTrace(
    operation = DestroyContractOperation(
        contractToDestroy = contractToDelete, refundContract = refundContract, refundValue = value
    ),
    result = if (error != null) ErroredOperationResult(error) else null,
    subtraces = emptyList()
)
