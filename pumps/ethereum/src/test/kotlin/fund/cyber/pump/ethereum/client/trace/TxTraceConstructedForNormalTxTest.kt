package fund.cyber.pump.ethereum.client.trace

import fund.cyber.search.model.ethereum.CallOperation
import fund.cyber.search.model.ethereum.CallOperationResult
import fund.cyber.search.model.ethereum.OperationTrace
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal

/**
 * Test regular tx (tx sends eth from one contract to another) should contain only one root call.
 */
class TxTraceConstructedForNormalTxTest : BaseTxTraceConverterTest() {

    private val expectedOperation = CallOperation(
        type = "call", from = "0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5", input = "0x", gasLimit = 29000,
        to = "0x00b654761a1b4372a77f6a327893572f55a483c7", value = BigDecimal("0.491554978522159616")
    )

    private val expectedOperationResult = CallOperationResult(output = "0x", gasUsed = 0)
    private val expectedRootOperationTrace = OperationTrace(expectedOperation, expectedOperationResult, emptyList())

    @Test
    @DisplayName("Should correctly construct TxTrace for normal tx")
    fun testTxTraceConstructedForNormalTx() {

        val txHash = "0xd7b10b163b1de8f8967d824ea73d996c476588a91a4c714ad897b135cf7fa4c5"
        val txTrace = constructTxTrace(txHash)
        val rootOperationTrace = txTrace.rootOperationTrace

        Assertions.assertEquals(1, txTrace.getAllOperationsTraces().size)
        Assertions.assertEquals(expectedRootOperationTrace, rootOperationTrace)
    }
}




