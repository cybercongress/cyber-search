package fund.cyber.dump.ethereum

import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord

fun <T> List<ConsumerRecord<PumpEvent, T>>.toRecordEventsMap(): Map<T, MutableList<PumpEvent>> {
    val blockEvents = mutableMapOf<T, MutableList<PumpEvent>>()
    this.forEach { record ->
        val events = blockEvents[record.value()]
        if (events != null) {
            events += record.key()
        } else {
            blockEvents[record.value()] = mutableListOf(record.key())
        }
    }

    return blockEvents
}

fun <T> Map<T, MutableList<PumpEvent>>.filterNotContainsAllEventsOf(
        events: Collection<PumpEvent>
): Map<T, MutableList<PumpEvent>> {
    return this.filterNot { entry ->
        entry.value.containsAll(events)
    }
}
