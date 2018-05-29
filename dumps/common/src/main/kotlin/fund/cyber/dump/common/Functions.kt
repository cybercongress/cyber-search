package fund.cyber.dump.common

import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

fun PumpEvent.changedFromNewBlock(prev: PumpEvent) = this != PumpEvent.NEW_BLOCK && prev == PumpEvent.NEW_BLOCK

fun PumpEvent.changedToNewBlock(prev: PumpEvent) = this == PumpEvent.NEW_BLOCK && prev != PumpEvent.NEW_BLOCK

fun <RECORD> List<ConsumerRecord<PumpEvent, RECORD>>.toFlux(
    recordToPublisher: (PumpEvent, RECORD) -> Publisher<*>
): Flux<Any> {

    fun RECORD.toPublisher(event: PumpEvent) = recordToPublisher(event, this)

    if (this.isEmpty()) return Flux.empty()

    var previousEvent = PumpEvent.NEW_BLOCK

    var resultFlux = Flux.empty<Any>()
    var currentFlux = Flux.empty<Any>()

    this.forEach { recordEvent ->

        val (event, record) = recordEvent.key() to recordEvent.value()

        val batchEnded = event.changedFromNewBlock(previousEvent) || event.changedToNewBlock(previousEvent)

        currentFlux = if (batchEnded) {
            resultFlux = resultFlux.concatWith(currentFlux)
            Flux.empty<Any>().mergeWith(record.toPublisher(event))
        } else {
            currentFlux.mergeWith(record.toPublisher(event))
        }

        previousEvent = event
    }

    return resultFlux.concatWith(currentFlux)
}
