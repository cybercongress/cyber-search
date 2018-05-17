package fund.cyber.pump.ethereum.client.trace

import fund.cyber.search.model.ethereum.CallOperation
import fund.cyber.search.model.ethereum.ErroredOperationResult
import fund.cyber.search.model.ethereum.OperationTrace
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal


class TxTraceConstructedForRevertedTxTest : BaseTxTraceConverterTest() {

    private val expectedOperation = CallOperation(
        type = "call", from = "0x2c2ff4ccd958954c5a5802c8cf81555d936f78ca", input = "0x", gasLimit = 69000,
        to = "0x71d442f6a361befb4e98e091a6bc31392abc65dc", value = BigDecimal("4.205780000000000000")
    )

    private val expectedOperationResult = ErroredOperationResult(error = "Reverted")
    private val expectedRootOperationTrace = OperationTrace(expectedOperation, expectedOperationResult, emptyList())

    @Test
    @DisplayName("Should correctly construct TxTrace for reverted tx")
    fun testTxTraceConstructedForRevertedTx() {

        val txHash = "0x581a5303beb776df3d363866a22de0e340ae6be63fe636f044983e685b060ea9"
        val txTrace = constructTxTrace(txHash)
        val rootOperationTrace = txTrace.rootOperationTrace

        Assertions.assertEquals(1, txTrace.getAllOperationsTraces().size)
        Assertions.assertEquals(expectedRootOperationTrace, rootOperationTrace)
    }
}
