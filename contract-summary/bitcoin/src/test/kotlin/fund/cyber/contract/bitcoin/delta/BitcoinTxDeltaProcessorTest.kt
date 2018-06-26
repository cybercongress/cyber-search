package fund.cyber.contract.bitcoin.delta

import fund.cyber.search.model.events.PumpEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class BitcoinTxDeltaTest {

    private val deltaProcessor = BitcoinTxDeltaProcessor()
    private val deltaMerger = BitcoinDeltaMerger()

    @Test
    fun affectedContractsTest() {
        val record1 = txRecord(PumpEvent.NEW_BLOCK, "a", "a", "b", BigDecimal("1.123"))
        val record2 = txRecord(PumpEvent.NEW_BLOCK, "b", "c", "d", BigDecimal("1.123"))
        val record3 = txRecord(PumpEvent.DROPPED_BLOCK, "b", "c", "d", BigDecimal("1.123"))

        val contracts = deltaProcessor.affectedContracts(listOf(record1, record2, record3))

        assertThat(contracts).hasSize(4)
        assertThat(contracts).containsExactlyInAnyOrder("a", "b", "c", "d")
    }

    @Test
    fun regularTxDeltaProcessorTest() {
        val record = txRecord(PumpEvent.NEW_BLOCK, "a", "a", "b", BigDecimal("1.123"))

        val deltas = deltaProcessor.recordToDeltas(record)

        assertThat(deltas).hasSize(3)
        assertThat(deltas[0]).isEqualTo(delta("a", BigDecimal("-2.223"), txNumberDelta = 1))
        assertThat(deltas[1]).isEqualTo(delta("b", BigDecimal("1.123"), txNumberDelta = 1))
        assertThat(deltas[2]).isEqualTo(delta("a", BigDecimal.ONE, txNumberDelta = 0))
    }

    @Test
    fun droppedTxDeltaProcessorTest() {
        val record = txRecord(PumpEvent.DROPPED_BLOCK, "a", "a", "b", BigDecimal("1.123"))

        val deltas = deltaProcessor.recordToDeltas(record)

        assertThat(deltas).hasSize(3)
        assertThat(deltas[0]).isEqualTo(delta("a", BigDecimal("-2.223"), true, -1))
        assertThat(deltas[1]).isEqualTo(delta("b", BigDecimal("1.123"), true, -1))
        assertThat(deltas[2]).isEqualTo(delta("a", BigDecimal.ONE, true, 0))
    }

    @Test
    fun deltaMergerTest() {
        val record1 = txRecord(PumpEvent.NEW_BLOCK, "a", "a", "b", BigDecimal("1.123"))
        val record2 = txRecord(PumpEvent.NEW_BLOCK, "b", "c", "d", BigDecimal("1.123"))
        val record3 = txRecord(PumpEvent.DROPPED_BLOCK, "b", "c", "d", BigDecimal("1.123"))

        val deltas = listOf(record1, record2, record3).flatMap { deltaProcessor.recordToDeltas(it) }

        assertThat(deltas).hasSize(9)

        val mergedDeltas = deltas
            .groupBy { delta -> delta.contract }
            .filterKeys { contract -> contract.isNotEmpty() }
            .values.mapNotNull { contractDeltas -> deltaMerger.mergeDeltas(contractDeltas, emptyMap()) }
            .sortedBy { d -> d.contract }

        assertThat(mergedDeltas).hasSize(4)

        assertThat(mergedDeltas[0]).isEqualTo(
            delta("a", BigDecimal("-1.223"), txNumberDelta = 1, totalReceived = BigDecimal("1"))
        )
        assertThat(mergedDeltas[1]).isEqualTo(
            delta("b", BigDecimal("1.123"), txNumberDelta = 1, totalReceived = BigDecimal("1.123"))
        )
        assertThat(mergedDeltas[2]).isEqualTo(
            delta("c", BigDecimal("0.000"), txNumberDelta = 0, totalReceived = BigDecimal("0"))
        )
        assertThat(mergedDeltas[3]).isEqualTo(
            delta("d", BigDecimal("0.000"), txNumberDelta = 0, totalReceived = BigDecimal("0.000"))
        )

    }

    @Test
    fun deltaMergerWithExistingRecordsTest() {
        val record1 = txRecord(PumpEvent.NEW_BLOCK, "a", "a", "b", BigDecimal("1.123"), 0)
        val record2 = txRecord(PumpEvent.NEW_BLOCK, "b", "c", "d", BigDecimal("1.123"), 2)
        val record3 = txRecord(PumpEvent.NEW_BLOCK, "c", "e", "f", BigDecimal("1.123"), 4)

        val currentContracts = mapOf(
            "a" to contractSummary("a", 0),
            "b" to contractSummary("b", 0),
            "c" to contractSummary("c", 2),
            "d" to contractSummary("d", 2)
        )

        val deltas = listOf(record1, record2, record3).flatMap { deltaProcessor.recordToDeltas(it) }

        assertThat(deltas).hasSize(9)

        val mergedDeltas = deltas
            .groupBy { delta -> delta.contract }
            .filterKeys { contract -> contract.isNotEmpty() }
            .values.mapNotNull { contractDeltas -> deltaMerger.mergeDeltas(contractDeltas, currentContracts) }
            .sortedBy { d -> d.contract }

        assertThat(mergedDeltas).hasSize(2)

        assertThat(mergedDeltas[0]).isEqualTo(
            delta("e", BigDecimal("-1.223"), txNumberDelta = 1, totalReceived = BigDecimal("1"), offset = 4)
        )
        assertThat(mergedDeltas[1]).isEqualTo(
            delta("f", BigDecimal("1.123"), txNumberDelta = 1, totalReceived = BigDecimal("1.123"), offset = 4)
        )
    }

}

