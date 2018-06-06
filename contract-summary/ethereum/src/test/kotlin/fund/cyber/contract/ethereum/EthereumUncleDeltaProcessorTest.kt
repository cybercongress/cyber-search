package fund.cyber.contract.ethereum

import fund.cyber.contract.ethereum.delta.EthereumContractSummaryDelta
import fund.cyber.contract.ethereum.delta.EthereumUncleDeltaProcessor
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.unclePumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant


@DisplayName("Ethereum uncle delta processor test: ")
class EthereumUncleDeltaProcessorTest {

    private val expectedDelta = EthereumContractSummaryDelta(
        contract = "0xea674fdde714fd979de3edf0f56aa9716b898ec8", balanceDelta = BigDecimal("1.875"),
        smartContract = null, totalReceivedDelta = BigDecimal("1.875"), txNumberDelta = 0,
        uncleNumberDelta = 1, minedBlockNumberDelta = 0,
        topic = EthereumFamilyChain.ETHEREUM.unclePumpTopic, partition = 0, offset = 0,
        lastOpTime = Instant.ofEpochMilli(100000)
    )

    private val expectedDroppedDelta = EthereumContractSummaryDelta(
        contract = "0xea674fdde714fd979de3edf0f56aa9716b898ec8", balanceDelta = BigDecimal("1.875").negate(),
        smartContract = null, totalReceivedDelta = BigDecimal("1.875").negate(), txNumberDelta = 0,
        uncleNumberDelta = -1, minedBlockNumberDelta = 0,
        topic = EthereumFamilyChain.ETHEREUM.unclePumpTopic, partition = 0, offset = 0,
        lastOpTime = Instant.ofEpochMilli(100000)
    )

    private val uncle = EthereumUncle(
            hash = "0xebeec27b1dc1f01bd6502a2c4ea62d58041d1f6fa6a5d1e18ec552dfd17558c3", position = 0,
            number = 5386263, timestamp = Instant.ofEpochMilli(100000), blockNumber = 5386266, blockTime = Instant.ofEpochMilli(100000),
            blockHash = "0xa27c04a1f42b2e5264e5cfb0cd1ca6fb84c360cbef63ea1b171906b1018e16dd",
            miner = "0xea674fdde714fd979de3edf0f56aa9716b898ec8", uncleReward = BigDecimal("1.875")
    )


    @Test
    @DisplayName("Should correctly convert ethereum uncle to contract deltas")
    fun testRecordToDeltas() {
        val record = ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0, 0, PumpEvent.NEW_BLOCK, uncle)

        val deltas = EthereumUncleDeltaProcessor().recordToDeltas(record)
        Assertions.assertEquals(deltas, listOf(expectedDelta))
    }

    @Test
    @DisplayName("Should correctly convert ethereum uncle with dropped block event to contract deltas")
    fun testRecordToDeltasDroppedBlock() {
        val record = ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0, 0, PumpEvent.DROPPED_BLOCK, uncle)

        val deltas = EthereumUncleDeltaProcessor().recordToDeltas(record)
        Assertions.assertEquals(deltas, listOf(expectedDroppedDelta))
    }

}

