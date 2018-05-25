@file:Suppress("LocalVariableName")

package fund.cyber.pump.ethereum.client.trace

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import fund.cyber.pump.ethereum.client.toTxesTraces
import fund.cyber.search.jsonSerializer
import fund.cyber.search.model.ethereum.ErroredOperationResult
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.web3j.protocol.parity.methods.response.Trace
import java.math.BigInteger.ONE
import java.math.BigInteger.ZERO

/**
 * Test tx trace should reduce stackoverlow brances to only parent.
 */
class TxTraceStackOverflowTest : BaseTxTraceConverterTest() {

    val txHash = "0x47bfefd0b1e6c055391e7b091425762322dde55ff72a9d3567b3c22a78b4a38a"

    @Test
    @DisplayName("All traces deeper than [MAX_TRACE_DEPTH], should be flattened into single sublist")
    fun testAllTracesDeeperThanLimitShouldBeFlattenIntoSingleParentSubtracesList() {

        val flattenTraces = getFlattenTracesWithDeepStack()
        val txTrace = toTxesTraces(flattenTraces)[txHash]!!
        val traceFor_0_0_0_0 = txTrace.rootOperationTrace.subtraces[0].subtraces[0].subtraces[0].subtraces[0]

        val txTraceAsString = jsonSerializer.writeValueAsString(txTrace)

        Assertions.assertEquals(1020, txTrace.getAllOperationsTraces().size)
        Assertions.assertEquals(12, txTrace.rootOperationTrace.subtraces.size)
        Assertions.assertTrue(txTrace.rootOperationTrace.subtraces[8].result is ErroredOperationResult)
        Assertions.assertEquals(998, traceFor_0_0_0_0.subtraces.size)
    }

    @Test
    @DisplayName("All errored traces, deeper than [MAX_TRACE_DEPTH], should be removed")
    fun testFailedTracesDeeperThanLimitShouldBeRemoved() {

        val flattenTraces = getFlattenTracesWithDeepFailedStack()
        val txTrace = toTxesTraces(flattenTraces)[txHash]!!
        val traceFor_0_0_0_0 = txTrace.rootOperationTrace.subtraces[0].subtraces[0].subtraces[0].subtraces[0]
        val traceFor_0_0_0_1 = txTrace.rootOperationTrace.subtraces[0].subtraces[0].subtraces[0].subtraces[1]

        val txTraceAsString = jsonSerializer.writeValueAsString(txTrace)

        // final tree should be:
        //                      root-0-0-(0 FAILED)
        //                                   | -> 0  OK     -> 199 dropped calls
        //                                   | -> 0  FAILED -> 998 dropped calls
        //
        Assertions.assertEquals(23, txTrace.getAllOperationsTraces().size)
        Assertions.assertEquals(12, txTrace.rootOperationTrace.subtraces.size)
        Assertions.assertEquals(0, traceFor_0_0_0_0.subtraces.size)
        Assertions.assertEquals(0, traceFor_0_0_0_1.subtraces.size)
        Assertions.assertEquals(199, traceFor_0_0_0_0.droppedSuboperationsNumber)
        Assertions.assertEquals(998, traceFor_0_0_0_1.droppedSuboperationsNumber)
    }

    @Test
    @DisplayName("If call have more than [SUBTRACES_NUMBER_BEFORE_ZIPPING] subtraces, all errored should be removed")
    fun testZipSubtracesForTraceWithALotOfSubtraces() {

        val flattenTraces = getFlattenTracesWithWideStack()
        val txTrace = toTxesTraces(flattenTraces)[txHash]!!
        val traceFor_0_0 = txTrace.rootOperationTrace.subtraces[0].subtraces[0]

        val txTraceAsString = jsonSerializer.writeValueAsString(txTrace)

        // final tree should be:
        //                      root-0-0
        //                              \ -> 500 sub  + 500 dropped
        //
        Assertions.assertEquals(520, txTrace.getAllOperationsTraces().size)
        Assertions.assertEquals(12, txTrace.rootOperationTrace.subtraces.size)
        Assertions.assertEquals(500, traceFor_0_0.subtraces.size)
        Assertions.assertEquals(500, traceFor_0_0.droppedSuboperationsNumber)
    }


    private fun getFlattenTracesWithDeepStack(): List<Trace> {
        val flattenTraces = getParityTracesForTx(txHash)
        val rootCall = flattenTraces.first()

        val subtracesFor_0_0 = (1..1000).map { depth ->
            return@map mock<Trace> {
                on { action } doReturn rootCall.action
                on { transactionHash } doReturn txHash
                on { result } doReturn rootCall.result
                on { subtraces } doReturn ONE
                on { traceAddress } doReturn Array(depth + 2, { ZERO }).toList()
            }
        }
        flattenTraces.addAll(3, subtracesFor_0_0)
        return flattenTraces
    }


    private fun getFlattenTracesWithDeepFailedStack(): List<Trace> {
        val flattenTraces = getParityTracesForTx(txHash)
        val rootCall = flattenTraces.first()

        //failed subtraces for root-0-0 call
        val subtracesFor_0_0 = (1..1000).map { depth ->
            return@map mock<Trace> {
                on { action } doReturn rootCall.action
                on { transactionHash } doReturn txHash
                on { error } doReturn if (depth % 100 == 0) null else "Reverted"
                on { result } doReturn if (depth % 100 == 0) rootCall.result else null
                on { subtraces } doReturn ONE
                on { traceAddress } doReturn Array(depth + 2, { ZERO }).toList()
            }
        }

        // ok subtraces for root-0-0-0 call
        // root-0-0-0 is call inserted previously
        // final tree is root-0-0-0
        //                        | -> 999 failed recursive calls
        //                        | -> 200 ok recursive calls
        val subtracesFor_0_0_0 = (1..200).map { depth ->
            return@map mock<Trace> {
                on { action } doReturn rootCall.action
                on { transactionHash } doReturn txHash
                on { result } doReturn rootCall.result
                on { subtraces } doReturn ONE
                on { traceAddress } doReturn Array(depth + 3, { ZERO }).toList()
            }
        }
        flattenTraces.addAll(3, subtracesFor_0_0)
        flattenTraces.addAll(4, subtracesFor_0_0_0)
        return flattenTraces
    }


    private fun getFlattenTracesWithWideStack(): List<Trace> {
        val flattenTraces = getParityTracesForTx(txHash)
        val rootCall = flattenTraces.first()

        // ok/failed subtraces for root-0-0 call
        val subtracesFor_0_0 = (0..999).map { depth ->
            return@map mock<Trace> {
                on { action } doReturn rootCall.action
                on { transactionHash } doReturn txHash
                on { error } doReturn if (depth % 2 == 0) null else "Reverted"
                on { result } doReturn if (depth % 2 == 0) rootCall.result else null
                on { subtraces } doReturn ONE
                on { traceAddress } doReturn listOf(ZERO, ZERO, depth.toBigInteger())
            }
        }
        flattenTraces.addAll(3, subtracesFor_0_0)
        return flattenTraces
    }
}
