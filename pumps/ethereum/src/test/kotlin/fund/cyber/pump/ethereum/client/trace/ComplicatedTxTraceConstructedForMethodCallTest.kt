package fund.cyber.pump.ethereum.client.trace

import fund.cyber.search.model.ethereum.ErroredOperationResult
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test


class ComplicatedTxTraceConstructedForMethodCallTest : BaseTxTraceConverterTest() {

    @Test
    @DisplayName("Should correctly construct TxTrace for complicated tx")
    fun testTxTraceConstructedForNormalTx() {

        val txHash = "0x47bfefd0b1e6c055391e7b091425762322dde55ff72a9d3567b3c22a78b4a38a"
        val txTrace = constructTxTrace(txHash)

        Assertions.assertEquals(20, txTrace.getAllOperationsTraces().size)
        Assertions.assertEquals(12, txTrace.rootOperationTrace.subtraces.size)
        Assertions.assertTrue(txTrace.rootOperationTrace.subtraces[8].result is ErroredOperationResult)
    }
}
