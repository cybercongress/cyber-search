package fund.cyber.pump.ethereum.client.trace

import fund.cyber.search.model.ethereum.CallOperation
import fund.cyber.search.model.ethereum.ErroredOperationResult
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal


class TxTraceConstructedForErroredTxTest : BaseTxTraceConverterTest() {

    private val expectedRootOperation = CallOperation(
        type = "call", from = "0x87268131fbcd127a0692ce6e0a714923fe6b67d1", input = "0xd1058e59", gas = 68728,
        to = "0xd0a6e6c54dbc68db5db3a091b171a77407ff7ccf", value = BigDecimal("0E-18")
    )

    private val expectedRootOperationResult = ErroredOperationResult(error = "Out of gas")

    @Test
    @DisplayName("Should correctly construct TxTrace for reverted tx")
    fun testTxTraceConstructedForErroredTx() {

        val txHash = "0x23562f848cb8b57b596e196c8f511c4ecc6cea38280edffa4bbc1e79aa413879"
        val txTrace = constructTxTrace(txHash)

        Assertions.assertEquals(2, txTrace.getAllOperationsTraces().size)
        Assertions.assertEquals(expectedRootOperation, txTrace.rootOperationTrace.operation)
        Assertions.assertEquals(expectedRootOperationResult, txTrace.rootOperationTrace.result)
    }
}
