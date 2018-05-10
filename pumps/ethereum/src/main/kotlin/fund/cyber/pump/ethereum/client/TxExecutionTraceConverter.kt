package fund.cyber.pump.ethereum.client

import fund.cyber.common.hexToLong
import fund.cyber.search.model.ethereum.CallOperation
import fund.cyber.search.model.ethereum.CreateContractOperation
import fund.cyber.search.model.ethereum.DestroyContractOperation
import fund.cyber.search.model.ethereum.Operation
import fund.cyber.search.model.ethereum.OperationResult
import fund.cyber.search.model.ethereum.OperationTrace
import fund.cyber.search.model.ethereum.TxTrace
import fund.cyber.search.model.ethereum.weiToEthRate
import org.web3j.protocol.parity.methods.response.Trace
import java.math.BigDecimal
import java.util.*

//todo add tests
const val EMPTY_FIELD = "0x"

/**
 * Trace is result of single "operation" inside transaction (ex: send eth to address inside smart contract method execution)
 * Parity return all traces for block as flatten list.
 * Current method gathers flatten traces list into stacktrace tree for each transaction
 *
 */
fun toTxesTraces(parityTraces: List<Trace>): List<TxTrace> {

    return parityTraces.removeSimpleTraces()
        .groupBy { trace -> trace.transactionHash }
        .map { (txHash, traces) -> toTxTrace(txHash, traces) }
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
private fun toTxTrace(txHash: String, traces: List<Trace>): TxTrace {

    val tree = mutableMapOf<Trace, MutableList<Trace>>()

    val parents = ArrayDeque<Trace>()
    for (trace in traces) {
        val node = trace
        // pop any parents from this deque as deep or deeper than this node
        while (parents.size > trace.traceAddress?.size ?: 0) parents.pop()
        // add node to tree
        tree[node] = mutableListOf()
        // add node to parent's children if applicable
        tree[parents.peek()]?.add(node)
        // add node to parents stack
        parents.push(node)
    }

    val rootOperationTrace = toOperationTrace(traces[0], tree)
    return TxTrace(txHash, rootOperationTrace)
}

/**
 * Converts raw parity trace and its child to search OperationTrace data class.
 * !!Recursive by child first.
 */
private fun toOperationTrace(trace: Trace, tracesTree: Map<Trace, List<Trace>>): OperationTrace {
    val subtraces = tracesTree[trace]?.map { subtrace -> toOperationTrace(subtrace, tracesTree) } ?: emptyList()
    val operation = convertOperation(trace.action)
    val result = convertResult(trace)
    return OperationTrace(operation, result, subtraces)
}


private fun convertResult(trace: Trace): OperationResult {
    val result = trace.result
    return OperationResult(
        error = trace.error, address = result?.address, code = result?.code,
        gasUsed = result?.gasUsedRaw?.hexToLong(), output = result?.output
    )
}

private fun convertOperation(action: Trace.Action): Operation {
    return when (action) {
        is Trace.CallAction -> {
            CallOperation(
                type = action.callType, from = action.from, to = action.to, input = action.input,
                value = BigDecimal(action.value) * weiToEthRate, gas = action.gasRaw.hexToLong()
            )
        }
        is Trace.CreateAction -> {
            CreateContractOperation(
                from = action.from, init = action.init,
                value = BigDecimal(action.value) * weiToEthRate, gas = action.gasRaw.hexToLong()
            )
        }
        is Trace.SuicideAction -> {
            DestroyContractOperation(
                address = action.address, refundAddress = action.refundAddress,
                balance = BigDecimal(action.balance) * weiToEthRate
            )
        }
        else -> throw RuntimeException()
    }
}


/**
 * Current method returns list of traces without simple traces.
 * Simple trace is a trace returned for regular tx(tx not invoking methods), block and uncles rewards.
 */
fun List<Trace>.removeSimpleTraces(): List<Trace> {

    return this.filter { call ->

        return@filter when (call.action) {
            is Trace.RewardAction -> false
            is Trace.CallAction -> !isSimpleCall(call.action as Trace.CallAction)
            else -> true
        }
    }
}

/**
 * Current method returns true if trace is simple call.
 * Simple call is a call returned for regular tx(tx not invoking methods).
 */
private fun isSimpleCall(call: Trace.CallAction): Boolean {
    return call.input == null || call.input.isEmpty() || EMPTY_FIELD == call.input
}