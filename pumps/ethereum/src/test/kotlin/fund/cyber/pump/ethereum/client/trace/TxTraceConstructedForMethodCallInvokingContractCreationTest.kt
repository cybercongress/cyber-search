package fund.cyber.pump.ethereum.client.trace

import fund.cyber.search.model.ethereum.CallOperation
import fund.cyber.search.model.ethereum.CallOperationResult
import fund.cyber.search.model.ethereum.CreateContractOperation
import fund.cyber.search.model.ethereum.CreateContractOperationResult
import fund.cyber.search.model.ethereum.OperationTrace
import fund.cyber.search.model.ethereum.TxTrace
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal

/**
 * Test method call, that invokes contract creation.
 *
 *  Tx -> contract method call
 *                \
 *                 --------> create contract
 */
class TxTraceConstructedForMethodCallInvokingContractCreationTest : BaseTxTraceConverterTest() {

    @Test
    @DisplayName("Should correctly construct TxTrace for method call, invoking contract creation")
    fun testTxTraceConstructedForMethodCallInvokingContractCreation() {

        val txHash = "0xbec2a4c10d10d657cb57f5790846025d2da271b10cdb929850831bba3a1ef62c"
        val txTrace = constructTxTrace(txHash)

        Assertions.assertEquals(expectedTxTrace(), txTrace)
    }


    private fun expectedTxTrace(): TxTrace {

        val expectedOperation = CallOperation(
            type = "call", from = "0xa7f3659c53820346176f7e0e350780df304db179", gasLimit = 476552,
            input = "0xce92dcede1c2010acd783af4f07bb862661e8a1b65f260baf1b64a5a09f62b631a67c5de",
            to = "0x6090a6e47849629b7245dfa1ca21d94cd15878ef", value = BigDecimal("1.030000000000000000")
        )

        val expectedOperationResult = CallOperationResult(output = "0x", gasUsed = 394988)
        val rootTrace = OperationTrace(
            expectedOperation, expectedOperationResult, listOf(createContractSuboperationTrace())
        )
        return TxTrace(rootTrace)
    }


    private fun createContractSuboperationTrace(): OperationTrace {

        val expectedOperation = CreateContractOperation(
            from = "0x6090a6e47849629b7245dfa1ca21d94cd15878ef", gasLimit = 436209,
            value = BigDecimal("1.030000000000000000"),
            init = "0x606060405236156100885763ffffffff60e060020a60003504166305b34410811461008a5780630b5ab3d5146100"
        )

        val expectedOperationResult = CreateContractOperationResult(
            address = "0xe8a3c515fd7d0914dc56a4e6dbbb77a7f58bc6ce", gasUsed = 339168,
            code = "0x606060405236156100885763ffffffff60e060020a60003504166305b34410811461008a5780630b5ab3d5146100"
        )

        return OperationTrace(expectedOperation, expectedOperationResult, emptyList())
    }
}
