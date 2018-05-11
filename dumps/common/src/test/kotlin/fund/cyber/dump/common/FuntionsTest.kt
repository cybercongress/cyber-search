package fund.cyber.dump.common

import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

data class TestData(
    private val hash: String,
    private val blockNumber: Long,
    private val data: String
)

class FunctionsTest {

    private val valueA = TestData("a", 1, "a")
    private val valueB = TestData("a", 1, "b")

    private val records = listOf(
        ConsumerRecord("topic", 0, 0, PumpEvent.NEW_BLOCK, valueA),
        ConsumerRecord("topic", 0, 2, PumpEvent.DROPPED_BLOCK, valueA),
        ConsumerRecord("topic", 0, 4, PumpEvent.NEW_BLOCK, valueB)
    )

    @Test
    fun toRecordsEventsMapTest() {

        val recordsEventsMap = records.toRecordEventsMap()

        Assertions.assertThat(recordsEventsMap).hasSize(2)
        Assertions.assertThat(recordsEventsMap.keys).containsExactly(valueA, valueB)
        Assertions.assertThat(recordsEventsMap[valueA]).containsExactly(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK)
        Assertions.assertThat(recordsEventsMap[valueB]).containsExactly(PumpEvent.NEW_BLOCK)
    }

    @Test
    fun filterNotContainsAllEventsOfTest() {

        val recordsEventsMap = records.toRecordEventsMap()

        val filtedByNewBlock = recordsEventsMap.filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK))

        Assertions.assertThat(filtedByNewBlock).isEmpty()

        val filtedByNewBlockAndDroppedBlock = recordsEventsMap
            .filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK))

        Assertions.assertThat(filtedByNewBlockAndDroppedBlock).hasSize(1)
        Assertions.assertThat(filtedByNewBlockAndDroppedBlock.keys).containsExactly(valueB)
        Assertions.assertThat(filtedByNewBlockAndDroppedBlock[valueB]).containsExactly(PumpEvent.NEW_BLOCK)
    }


}
