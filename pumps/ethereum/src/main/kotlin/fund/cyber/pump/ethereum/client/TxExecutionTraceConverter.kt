package fund.cyber.pump.ethereum.client

import fund.cyber.common.hexToLong
import fund.cyber.common.isEmptyHexValue
import fund.cyber.common.toSearchHashFormat
import fund.cyber.search.model.ethereum.CallOperation
import fund.cyber.search.model.ethereum.CallOperationResult
import fund.cyber.search.model.ethereum.CreateContractOperation
import fund.cyber.search.model.ethereum.CreateContractOperationResult
import fund.cyber.search.model.ethereum.DestroyContractOperation
import fund.cyber.search.model.ethereum.ErroredOperationResult
import fund.cyber.search.model.ethereum.Operation
import fund.cyber.search.model.ethereum.OperationResult
import fund.cyber.search.model.ethereum.OperationTrace
import fund.cyber.search.model.ethereum.RewardOperation
import fund.cyber.search.model.ethereum.TxTrace
import fund.cyber.search.model.ethereum.weiToEthRate
import org.web3j.protocol.parity.methods.response.Trace
import org.web3j.protocol.parity.methods.response.Trace.CallAction
import org.web3j.protocol.parity.methods.response.Trace.CreateAction
import org.web3j.protocol.parity.methods.response.Trace.RewardAction
import org.web3j.protocol.parity.methods.response.Trace.SuicideAction
import java.math.BigDecimal
import java.util.*


const val MAX_TRACE_DEPTH = 5
const val SUBTRACES_NUMBER_BEFORE_ZIPPING = 14

const val CREATE_CONTRACT_ERROR = "Contract creation error"

/**
 * Trace(parity) is result of single "operation/call" inside transaction (ex: send eth to address inside smart contract
 *  method execution). Parity return all traces for block(tx) as flatten list.
 *  Current method gathers flatten traces list into trace tree for transaction.
 *
 * For normal txes, first(root) operation(call) duplicate parent tx data (such as value, from, to, etc).
 * For method execution txes, first(root) operation(call) duplicate parent tx data (such as value, from, to, etc).
 * For sm creation/deletion first(root) operation(call) duplicate parent tx data (such as value, from, to, etc).
 *
 *
 * IMPORTANT NOTE:
 * 1) We do not store all traces. All errored traces, deeper than [MAX_TRACE_DEPTH], will be removed.
 * 2) All traces deeper than [MAX_TRACE_DEPTH], will be flattened into single sublist.
 * 3) Also, if node have more than [SUBTRACES_NUMBER_BEFORE_ZIPPING] subtraces, all errored will be removed.
 * 4) We do not store byte code for not created smart contract.
 */
fun toTxesTraces(parityTraces: List<Trace>): Map<String, TxTrace> {

    return parityTraces
        .groupBy { trace -> trace.transactionHash }
        .mapValues { (_, traces) -> toTxTrace(traces) }
}

/**
 * Returns tx flatten list of traces as single stacktrace tree.
 * Current assumptions. traces list is ordered according to scheme:
 *  (root)
 *  (child1)
 *  (child2)
 *  (child2-child1)
 *  (child2-child1-child1)
 *  (child2-child2)
 *  (child3)
 *  etc
 *
 *  In raw trace, tree index is represented by array traceAddress. See https://wiki.parity.io/JSONRPC-trace-module
 */
private fun toTxTrace(traces: List<Trace>): TxTrace {

    val tree = mutableMapOf<Trace, MutableList<Trace>>()

    // pop any parents from this deque as deep or deeper than this node
    // add node to tree
    // add node to parent's children if applicable
    // add node to parents stack
    val parents = ArrayDeque<Trace>()
    for (trace in traces) {
        while (parents.size > trace.traceAddress?.size ?: 0) parents.pop()
        tree[trace] = mutableListOf()
        tree[parents.peek()]?.add(trace)
        parents.push(trace)
    }

    val rootOperationTrace = toOperationTrace(traces[0], tree, 0)
    return TxTrace(rootOperationTrace)
}

/**
 * Converts raw parity trace and its child to search OperationTrace data class.
 *
 * IMPORTANT NOTE:
 * !!Recursive by child first.
 * !!All traces deeper than [MAX_TRACE_DEPTH], will be flattened into single sublist.
 *
 */
private fun toOperationTrace(
    trace: Trace, tracesTree: Map<Trace, List<Trace>>, depthFromRoot: Int, isParentFailed: Boolean = false
): OperationTrace {

    // -1 due include root
    return if (depthFromRoot < MAX_TRACE_DEPTH - 1) {
        toOpTraceAsTree(trace, tracesTree, isParentFailed, depthFromRoot)
    } else {
        toOpTraceAsFlattenList(trace, tracesTree, isParentFailed)
    }
}


/**
 *  All errored traces with their subtraces will be not returned.
 */
private fun toOpTraceAsFlattenList(
    trace: Trace, tracesTree: Map<Trace, List<Trace>>, isParentFailed: Boolean
): OperationTrace {

    val operation = convertOperation(trace)
    val result = convertResult(trace)

    return if (isParentFailed) {
        OperationTrace(operation, result, emptyList(), getSubtracesNumber(trace, tracesTree))
    } else {
        val subtracesToStore = getAllSuccessfulSubtracesAsFlattenList(trace, tracesTree).map { subtrace ->
            OperationTrace(convertOperation(subtrace), convertResult(subtrace))
        }
        val droppedTracesNumber = getSubtracesNumber(trace, tracesTree) - subtracesToStore.size

        OperationTrace(operation, result, subtracesToStore, droppedTracesNumber)
    }
}

/**
 * If node have more than [SUBTRACES_NUMBER_BEFORE_ZIPPING] subtraces, all errored one will be removed.
 */
private fun toOpTraceAsTree(
    trace: Trace, tracesTree: Map<Trace, List<Trace>>, isParentFailed: Boolean, depthFromRoot: Int
): OperationTrace {

    val operation = convertOperation(trace)
    val result = convertResult(trace)
    val childIsParentFailed = isParentFailed || result is ErroredOperationResult

    val children = tracesTree[trace] ?: return OperationTrace(operation, result, emptyList())

    val subtraces = children.map { subtrace ->
        toOperationTrace(subtrace, tracesTree, depthFromRoot + 1, childIsParentFailed)
    }
    if (subtraces.size > SUBTRACES_NUMBER_BEFORE_ZIPPING) {
        val subtracesToStore = subtraces.filterNot(OperationTrace::isOperationFailed)
        val droppedTracesNumber = subtraces.size - subtracesToStore.size
        return OperationTrace(operation, result, subtracesToStore, droppedTracesNumber)
    }
    return OperationTrace(operation, result, subtraces)
}


private fun getSubtracesNumber(trace: Trace, tracesTree: Map<Trace, List<Trace>>): Int {
    return tracesTree[trace]?.map { subtrace -> getSubtracesNumber(subtrace, tracesTree) + 1 }?.sum() ?: 0
}


private fun getAllSuccessfulSubtracesAsFlattenList(
    trace: Trace, tracesTree: Map<Trace, List<Trace>>
): List<Trace> {

    val children = tracesTree[trace] ?: emptyList()
    return children.flatMap { subtrace ->
        if (subtrace.error == null || subtrace.error.isEmpty()) {
            listOf(subtrace) + getAllSuccessfulSubtracesAsFlattenList(subtrace, tracesTree)
        } else {
            emptyList()
        }
    }
}


private fun convertResult(trace: Trace): OperationResult? {

    if (trace.error != null && trace.error.isNotEmpty()) return ErroredOperationResult(trace.error)
    if (trace.result == null) return null

    val result = trace.result
    val gasUsed = result.gasUsedRaw.hexToLong()
    return when (trace.action) {
        is CallAction -> CallOperationResult(gasUsed, result.output)
        is CreateAction -> {
            if (trace.isFailed()) ErroredOperationResult(CREATE_CONTRACT_ERROR)
            else CreateContractOperationResult(result.address.toSearchHashFormat(), result.code, gasUsed)
        }
        else -> throw RuntimeException("Unknown trace call result")
    }
}

private fun convertOperation(trace: Trace): Operation {
    val action = trace.action
    return when (action) {
        is CallAction -> {
            CallOperation(
                type = action.callType, from = action.from.toSearchHashFormat(),
                to = action.to.toSearchHashFormat(), input = action.input,
                value = BigDecimal(action.value) * weiToEthRate, gasLimit = action.gasRaw.hexToLong()
            )
        }
        is CreateAction -> {
            CreateContractOperation(
                from = action.from.toSearchHashFormat(), init = if (trace.isFailed()) "" else action.init,
                value = BigDecimal(action.value) * weiToEthRate, gasLimit = action.gasRaw.hexToLong()
            )
        }
        is SuicideAction -> {
            DestroyContractOperation(
                contractToDestroy = action.address.toSearchHashFormat(),
                refundContract = action.refundAddress.toSearchHashFormat(),
                refundValue = BigDecimal(action.balance) * weiToEthRate
            )
        }
        is RewardAction -> {
            RewardOperation(
                action.author.toSearchHashFormat(), BigDecimal(action.value) * weiToEthRate, action.rewardType
            )
        }
        else -> throw RuntimeException("Unknown trace call")
    }
}

/**
 * returns "0x" for contract as indicator of failed contract creation
 */
fun Trace.isFailed(): Boolean = when {
    error != null && error.isNotEmpty() -> true
    action is CreateAction && (result.code == null || result.code.isEmpty() || result.code.isEmptyHexValue()) -> true
    else -> false
}

