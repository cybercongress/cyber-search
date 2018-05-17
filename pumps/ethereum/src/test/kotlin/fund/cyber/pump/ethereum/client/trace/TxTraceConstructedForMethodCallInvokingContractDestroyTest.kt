package fund.cyber.pump.ethereum.client.trace

import fund.cyber.search.model.ethereum.DestroyContractOperation
import fund.cyber.search.model.ethereum.OperationTrace
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal


/**
 * Test method call, that invokes contract creation.
 *
 *  Tx -> contract method call
 *                \
 *                 --------> destroy contract
 */
class TxTraceConstructedForMethodCallInvokingContractDestroyTest : BaseTxTraceConverterTest() {

    @Test
    @DisplayName("Should correctly construct TxTrace for method call, invoking contract destroy")
    fun testTxTraceConstructedForMethodCallInvokingContractCreation() {

        val txHash = "0x47f7cff7a5e671884629c93b368cb18f58a993f4b19c2a53a8662e3f1482f690"
        val txTrace = constructTxTrace(txHash)

        Assertions.assertEquals(2, txTrace.getAllOperationsTraces().size)
        Assertions.assertEquals(1, txTrace.rootOperationTrace.subtraces.size)
        Assertions.assertEquals(expectedDestroyContractSuboperationTrace(), txTrace.rootOperationTrace.subtraces[0])
    }

    private fun expectedDestroyContractSuboperationTrace(): OperationTrace {

        val operation = DestroyContractOperation(
            contractToDestroy = "0x863df6bfa4469f3ead0be8f9f2aae51c91a907b4", refundValue = BigDecimal("0E-18"),
            refundContract = "0xae7168deb525862f4fee37d987a971b385b96952"
        )

        return OperationTrace(operation, null, emptyList())
    }
}
