package fund.cyber.contract.ethereum

import fund.cyber.contract.ethereum.summary.EthereumContractSummaryDelta
import fund.cyber.contract.ethereum.summary.EthereumTxDeltaProcessor
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant


@DisplayName("Ethereum transaction delta processor test: ")
class EthereumTxDeltaProcessorTest {

    private val expectedFirstDelta = EthereumContractSummaryDelta(
            contract = "0x4585c7eaa2cb96d4b59e868929efabeeb8e65b07", balanceDelta = BigDecimal("0.800483").negate(),
            smartContract = false, totalReceivedDelta = BigDecimal.ZERO, txNumberDelta = 1, uncleNumberDelta = 0, minedBlockNumberDelta = 0,
            topic = EthereumFamilyChain.ETHEREUM.txPumpTopic, partition = 0, offset = 0
    )

    private val expectedSecondDelta = EthereumContractSummaryDelta(
            contract = "0x39a629145280fd28b74b878e44d6fed7bd4dffe5", balanceDelta = BigDecimal("0.8"),
            smartContract = false, totalReceivedDelta = BigDecimal("0.8"), txNumberDelta = 1, uncleNumberDelta = 0, minedBlockNumberDelta = 0,
            topic = EthereumFamilyChain.ETHEREUM.txPumpTopic, partition = 0, offset = 0
    )

    private val expectedFirstDroppedDelta = EthereumContractSummaryDelta(
            contract = "0x4585c7eaa2cb96d4b59e868929efabeeb8e65b07", balanceDelta = BigDecimal("0.800483"),
            smartContract = false, totalReceivedDelta = BigDecimal.ZERO, txNumberDelta = -1, uncleNumberDelta = 0, minedBlockNumberDelta = 0,
            topic = EthereumFamilyChain.ETHEREUM.txPumpTopic, partition = 0, offset = 0
    )

    private val expectedSecondDroppedDelta = EthereumContractSummaryDelta(
            contract = "0x39a629145280fd28b74b878e44d6fed7bd4dffe5", balanceDelta = BigDecimal("0.8").negate(),
            smartContract = false, totalReceivedDelta = BigDecimal("0.8").negate(), txNumberDelta = -1, uncleNumberDelta = 0, minedBlockNumberDelta = 0,
            topic = EthereumFamilyChain.ETHEREUM.txPumpTopic, partition = 0, offset = 0
    )

    private val tx = EthereumTx(
            hash = "0x64c95bb2b75068ae92c36c5e3888e61af5de4398dd6eab64013cc1f96fd2ccf3",
            nonce = 0, blockHash = "0x97bda148dc1c0181bfca62d0af4443e736a65c8b2909062abd8c8c6aa9e62d85",
            blockNumber = 4959189, blockTime = Instant.now(), positionInBlock = 1,
            from = "0x4585c7eaa2cb96d4b59e868929efabeeb8e65b07", to = "0x39a629145280fd28b74b878e44d6fed7bd4dffe5",
            value = BigDecimal("0.8"), gasPrice = BigDecimal("0.000000023"), gasLimit = 21000L,
            gasUsed = 21000L, fee = BigDecimal("0.000483"), input = "", createdContract = null
    )

    @Test
    @DisplayName("Should correctly convert ethereum transaction to contract deltas")
    fun testRecordToDeltas() {
        val record = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0, 0, PumpEvent.NEW_BLOCK, tx)

        val deltas = EthereumTxDeltaProcessor().recordToDeltas(record)
        Assertions.assertEquals(deltas, listOf(expectedFirstDelta, expectedSecondDelta))
    }

    @Test
    @DisplayName("Should correctly convert ethereum transaction with dropped block event to contract deltas")
    fun testRecordToDeltasDroppedBlock() {
        val record = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0, 0, PumpEvent.DROPPED_BLOCK, tx)

        val deltas = EthereumTxDeltaProcessor().recordToDeltas(record)
        Assertions.assertEquals(deltas, listOf(expectedFirstDroppedDelta, expectedSecondDroppedDelta))
    }

}

