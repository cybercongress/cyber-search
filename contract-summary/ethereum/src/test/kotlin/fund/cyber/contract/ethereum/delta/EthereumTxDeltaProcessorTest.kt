package fund.cyber.contract.ethereum.delta

import fund.cyber.search.model.ethereum.OperationType.CALL
import fund.cyber.search.model.ethereum.OperationType.CREATE_CONTRACT
import fund.cyber.search.model.ethereum.TxTrace
import fund.cyber.search.model.events.PumpEvent.DROPPED_BLOCK
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigDecimal.ZERO


@DisplayName("Ethereum transaction delta processor test: ")
class EthereumTxDeltaProcessorTest {

    private val deltaProcessor = EthereumTxDeltaProcessor()

    /**
     * When sending ETH from one simple contract to other simple contract (not smart contracts), there is still single
     *  `call` operation.
     *
     *  Tx -> Ok Call
     */
    @Test
    @DisplayName("should correctly convert regular tx(contract-to-contract) to deltas")
    fun testContractToContractTxToDeltas() {

        val txTrace = TxTrace(rootOperationTrace(value = BigDecimal("0.8")))
        val txRecord = txRecord(BigDecimal("0.8"), trace = txTrace)
        val txDroppedRecord = txRecord(BigDecimal("0.8"), DROPPED_BLOCK, trace = txTrace)

        val expectedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.80046").negate(), txDelta = true),
            delta(rootTxTo, BigDecimal("0.8"), txDelta = true)
        )
        val expectedDroppedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.80046").negate(), reverted = true, txDelta = true),
            delta(rootTxTo, BigDecimal("0.8"), reverted = true, txDelta = true)
        )
        val actualDeltas = deltaProcessor.recordToDeltas(txRecord)
        val actualDroppedDeltas = deltaProcessor.recordToDeltas(txDroppedRecord)

        Assertions.assertEquals(expectedDeltas, actualDeltas)
        Assertions.assertEquals(expectedDroppedDeltas, actualDroppedDeltas)
    }

    /**
     * There is a case, when sending funds to contract can trigger not producing other operations `Fallback Function`,
     *  that will modify internal storage.
     *
     *  Tx -> Ok Call
     */
    @Test
    @DisplayName("should correctly convert regular tx(contract-to-smart_contract) to deltas")
    fun testContractToSmartContractSimpleTxWithoutValueToDeltas() {

        val txTrace = TxTrace(rootOperationTrace())
        val txRecord = txRecord(ZERO, trace = txTrace)

        val expectedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.00046").negate(), txDelta = true),
            delta(rootTxTo, ZERO, txDelta = true)
        )
        val actualDeltas = deltaProcessor.recordToDeltas(txRecord)
        Assertions.assertEquals(expectedDeltas, actualDeltas)
    }

    /**
     * There is a case, when sending funds to contract can trigger not producing other operations `Fallback Function`,
     *  that will modify internal storage.
     *
     *  Tx -> Ok Call
     */
    @Test
    @DisplayName("should correctly convert regular tx(contract-to-smart_contract) to deltas")
    fun testContractToSmartContractSimpleTxWithValueToDeltas() {

        val txTrace = TxTrace(rootOperationTrace(CALL, BigDecimal("0.8")))
        val txRecord = txRecord(BigDecimal("0.8"), trace = txTrace)

        val expectedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.80046").negate(), txDelta = true),
            delta(rootTxTo, BigDecimal("0.8"), txDelta = true)
        )
        val actualDeltas = deltaProcessor.recordToDeltas(txRecord)
        Assertions.assertEquals(expectedDeltas, actualDeltas)
    }

    /**
     * There is a case, when sending funds to contract can trigger `Fallback Function`. If enough gas was provided,
     *  fallback function can execute others [fund.cyber.search.model.ethereum.OperationType] changing balances.
     *
     *  Tx -> Ok Call
     *            | -> Ok Call
     *            | -> Fail Call
     *            | -> Ok Call
     */
    @Test
    @DisplayName("should correctly convert regular tx(contract-to-smart_contract) to deltas")
    fun testContractToSmartContractComplicatedTxToDeltas() {

        val subtraces = listOf(
            callOperationTrace(value = BigDecimal("0.8")), callOperationTrace(error = "fail"), callOperationTrace()
        )
        val txTrace = TxTrace(rootOperationTrace(subtraces = subtraces))
        val txRecord = txRecord(value = ZERO, trace = txTrace)

        val expectedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.00046").negate(), txDelta = true),
            delta(rootTxTo, ZERO, txDelta = true),
            delta(txFrom, BigDecimal("0.8").negate()),
            delta(txTo, BigDecimal("0.8")),
            delta(txFrom, ZERO),
            delta(txTo, ZERO)
        )
        val actualDeltas = deltaProcessor.recordToDeltas(txRecord)
        Assertions.assertEquals(expectedDeltas, actualDeltas)
    }

    /**
     * Invoking create smart contract not producing other operations will produce single root create sm operation.
     *
     *  Tx -> Create Call
     */
    @Test
    @DisplayName("should correctly convert tx creating smart contract(only one root operation) to deltas")
    fun testCreatingSmartContractSimpleTxToDeltas() {
        val txTrace = TxTrace(rootOperationTrace(CREATE_CONTRACT, value = BigDecimal("0.8")))
        val txRecord = txRecord(value = BigDecimal("0.8"), trace = txTrace)
        val expectedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.80046").negate(), txDelta = true),
            delta(rootTxTo, BigDecimal("0.8"), txDelta = true, smartContract = true)
        )
        val actualDeltas = deltaProcessor.recordToDeltas(txRecord)
        Assertions.assertEquals(expectedDeltas, actualDeltas)
    }

    /**
     * Invoking create smart contract not producing other operations will produce single root create sm operation.
     *
     *  Tx -> Fail Create
     */
    @Test
    @DisplayName("should correctly convert failed tx creating smart contract(only one root operation) to deltas")
    fun testCreatingSmartContractFailedSimpleTxToDeltas() {
        val txTrace = TxTrace(rootOperationTrace(CREATE_CONTRACT, value = BigDecimal("0.8"), error = "outOfGas"))
        val txRecord = txRecord(value = BigDecimal("0.8"), trace = txTrace)
        val expectedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.00046").negate(), txDelta = true, opDelta = false)
        )
        val actualDeltas = deltaProcessor.recordToDeltas(txRecord)
        Assertions.assertEquals(expectedDeltas, actualDeltas)
    }

    /**
     * Invoking destroy smart contract not producing other operations will produce single root destroy sm operation.
     *
     *  Tx -> Ok Call
     *            | -> Ok Destroy
     */
    @Test
    @DisplayName("should correctly convert tx creating smart contract(only one root operation) to deltas")
    fun testDestroySmartContractSimpleTxToDeltas() {
        val subtraces = listOf(destroyContractOperationTrace(value = BigDecimal("0.8")))
        val txTrace = TxTrace(rootOperationTrace(subtraces = subtraces))
        val txRecord = txRecord(value = BigDecimal("0.8"), trace = txTrace)
        val expectedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.00046").negate(), txDelta = true),
            delta(rootTxTo, ZERO, txDelta = true),
            delta(rootTxTo, BigDecimal("0.8").negate()),
            delta(txTo, BigDecimal("0.8"))
        )
        val actualDeltas = deltaProcessor.recordToDeltas(txRecord)
        Assertions.assertEquals(expectedDeltas, actualDeltas)
    }

    /**
     * Invoking destroy smart contract not producing other operations will produce single root destroy sm operation.
     *
     *  Tx -> Fail Call
     *            | -> Ok Destroy
     */
    @Test
    @DisplayName("should correctly convert tx creating smart contract(only one root operation) to deltas")
    fun testDestroySmartContractFailedSimpleTxToDeltas() {
        val subtraces = listOf(destroyContractOperationTrace(value = BigDecimal("0.8")))
        val txTrace = TxTrace(rootOperationTrace(subtraces = subtraces, error = "out of gas"))
        val txRecord = txRecord(value = BigDecimal("0.8"), trace = txTrace)
        val expectedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.00046").negate(), txDelta = true, opDelta = false),
            delta(rootTxTo, ZERO, txDelta = true, opDelta = false)
        )
        val actualDeltas = deltaProcessor.recordToDeltas(txRecord)
        Assertions.assertEquals(expectedDeltas, actualDeltas)
    }

    /**
     * Some sub-operations can be error, but main contract invocation can handle such cases and still succeeded.
     *
     *  Tx -> Ok Call
     *            | -> Ok Call
     *                     |  -> Ok Call
     *                     |  -> Ok Create
     *                     |  -> Fail Call
     *                                 | -> Ok Destroy
     *            | -> Fail Call
     *            | -> Ok Call
     */
    @Test
    @DisplayName("should correctly convert succeeded method invocation, that produce some errored operations to deltas")
    fun testInvokingMethodProducingSomeErroredOperationsToDeltas() {
        val subtraces13 = listOf(destroyContractOperationTrace(value = BigDecimal("0.8")))
        val subtraces1 = listOf(
            callOperationTrace(), createContractOperationTrace(value = BigDecimal("0.4")),
            callOperationTrace(error = "e", subtraces = subtraces13)
        )
        val subtraces = listOf(
            callOperationTrace(subtraces = subtraces1), callOperationTrace(error = "e"), callOperationTrace()
        )
        val txTrace = TxTrace(rootOperationTrace(value = BigDecimal("0.8"), subtraces = subtraces))

        val txRecord = txRecord(value = BigDecimal("0.8"), trace = txTrace)
        val expectedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.80046").negate(), txDelta = true),  // [1]
            delta(rootTxTo, BigDecimal("0.8"), txDelta = true),                 // [1]
            delta(txFrom, ZERO),                                                    // [1-1]
            delta(txTo, ZERO),                                                      // [1-1]
            delta(txFrom, ZERO),                                                    // [1-1-1]
            delta(txTo, ZERO),                                                      // [1-1-1]
            delta(txFrom, BigDecimal("0.4").negate()),                          // [1-1-2]
            delta(txTo, BigDecimal("0.4"), smartContract = true),               // [1-1-2]
            delta(txFrom, ZERO),                                                    // [1-3]
            delta(txTo, ZERO)                                                       // [1-3]
        )
        val actualDeltas = deltaProcessor.recordToDeltas(txRecord)
        Assertions.assertEquals(expectedDeltas, actualDeltas)
    }

    /**
     * Invoking method can fail due some reasons ( like out of gas). All state reverted back, all attached gas
     *  is drained.
     *
     *  Tx -> Fail Call
     *            | -> Ok Call
     *                     |  -> Ok Call
     *                     |  -> Ok Create
     *                     |  -> Fail Call
     *                                 | -> Ok Destroy
     *            | -> Fail Call
     *            | -> Ok Call
     */
    @Test
    @DisplayName("should correctly convert regular failed tx(contract-to-contract) to no deltas")
    fun testInvokingMethodRootWithOpFailedTxToDeltas() {
        val subtraces13 = listOf(destroyContractOperationTrace(value = BigDecimal("0.8")))
        val subtraces1 = listOf(
            callOperationTrace(), createContractOperationTrace(value = BigDecimal("0.4")),
            callOperationTrace(error = "e", subtraces = subtraces13)
        )
        val subtraces = listOf(
            callOperationTrace(subtraces = subtraces1), callOperationTrace(error = "e"), callOperationTrace()
        )
        val txTrace = TxTrace(rootOperationTrace(value = BigDecimal("0.8"), subtraces = subtraces, error = "e"))

        val txRecord = txRecord(value = BigDecimal("0.8"), trace = txTrace)
        val expectedDeltas = listOf(
            delta(rootTxFrom, BigDecimal("0.00046").negate(), txDelta = true, opDelta = false),
            delta(rootTxTo, ZERO, txDelta = true, opDelta = false)
        )
        val actualDeltas = deltaProcessor.recordToDeltas(txRecord)
        Assertions.assertEquals(expectedDeltas, actualDeltas)
    }

    /**
     *
     *  Tx -> Ok Call
     *            | -> Ok Call
     *                     |  -> Ok Call
     *                     |  -> Ok Create
     *                     |  -> Fail Call
     *                                 | -> Ok Destroy
     *            | -> Fail Call
     *            | -> Ok Call
     */
    @Test
    @DisplayName("affected addresses should returns same contracts as in record deltas")
    fun testAffectedAddressShouldReturnsSameContractsAsDeltas() {
        val subtraces13 = listOf(
            destroyContractOperationTrace(contractToDelete = "1-1-3-1f", refundContract = "1-1-3-1t")
        )
        val subtraces1 = listOf(
            callOperationTrace(from = "1-1-1f", to = "1-1-1t"),
            createContractOperationTrace(from = "1-1-2f", to = "1-1-2t"),
            callOperationTrace(from = "1-1-3f", to = "1-1-3t", error = "e", subtraces = subtraces13)
        )
        val subtraces = listOf(
            callOperationTrace(from = "1-1f", to = "1-1t", subtraces = subtraces1),
            callOperationTrace(from = "1-2f", to = "1-2t", error = "e"),
            callOperationTrace(from = "1-3f", to = "1-3t")
        )
        val txTrace = TxTrace(
            callOperationTrace(from = "1f", to = "1t", subtraces = subtraces)
        )

        val txRecord = txRecord(trace = txTrace, value = ZERO)

        val expectedContracts = setOf("1f", "1t", "1-1f", "1-1t", "1-1-1f", "1-1-1t", "1-1-2f", "1-1-2t", "1-3f", "1-3t")
        val affectedContracts = deltaProcessor.affectedContracts(listOf(txRecord))
        val deltasContracts = deltaProcessor.recordToDeltas(txRecord).map { d -> d.contract }.toSet()

        Assertions.assertEquals(expectedContracts, affectedContracts)
        Assertions.assertEquals(expectedContracts, deltasContracts)
    }

    /**
     *
     *  Tx -> Ok Fail
     *            | -> Ok Call
     *            | -> Fail Call
     *            | -> Ok Call
     */
    @Test
    @DisplayName("affected addresses should returns same contracts as in record deltas for failed root tx")
    fun testAffectedAddressShouldReturnsSameContractsAsDeltasForFailedRootTx() {
        val subtraces = listOf(
            callOperationTrace(from = "1-1f", to = "1-1t"),
            callOperationTrace(from = "1-2f", to = "1-2t", error = "e"),
            callOperationTrace(from = "1-3f", to = "1-3t")
        )
        val txTrace = TxTrace(
            callOperationTrace(from = "1f", to = "1t", subtraces = subtraces, error = "e")
        )

        val txRecord = txRecord(trace = txTrace, value = ZERO)

        val expectedContracts = setOf("1f", "1t")
        val affectedContracts = deltaProcessor.affectedContracts(listOf(txRecord))
        val deltasContracts = deltaProcessor.recordToDeltas(txRecord).map { d -> d.contract }.toSet()

        Assertions.assertEquals(expectedContracts, affectedContracts)
        Assertions.assertEquals(expectedContracts, deltasContracts)
    }
}

