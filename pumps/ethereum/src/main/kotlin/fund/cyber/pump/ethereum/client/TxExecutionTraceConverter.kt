package fund.cyber.pump.ethereum.client

import fund.cyber.common.hexToLong
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
import java.math.BigDecimal
import java.util.*

/**
 * Trace(parity) is result of single "operation/call" inside transaction (ex: send eth to address inside smart contract
 *  method execution). Parity return all traces for block(tx) as flatten list.
 *  Current method gathers flatten traces list into trace tree for transaction.
 *
 * For normal txes, first(root) operation(call) duplicate parent tx data (such as value, from, to, etc).
 * For method execution txes, first(root) operation(call) duplicate parent tx data (such as value, from, to, etc).
 * For sm creation/deletion first(root) operation(call) duplicate parent tx data (such as value, from, to, etc).
 *
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

    val rootOperationTrace = toOperationTrace(traces[0], tree)
    return TxTrace(rootOperationTrace)
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


private fun convertResult(trace: Trace): OperationResult? {

    if (trace.error != null && trace.error.isNotEmpty()) return ErroredOperationResult(trace.error)
    if (trace.result == null) return null

    val result = trace.result
    val gasUsed = result.gasUsedRaw.hexToLong()
    return when (trace.action) {
        is Trace.CallAction -> CallOperationResult(gasUsed, result.output)
        is Trace.CreateAction -> CreateContractOperationResult(result.address, result.code, gasUsed)
        else -> throw RuntimeException("Unknown trace call result")
    }
}

//todo check weiToEthRate value result
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
        is Trace.RewardAction -> {
            RewardOperation(action.author, BigDecimal(action.value) * weiToEthRate, action.rewardType)
        }
        else -> throw RuntimeException("Unknown trace call")
    }
}
