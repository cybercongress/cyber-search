package fund.cyber.dump.common

import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux


fun List<Flux<Any>>.execute() {

    this.forEach { flux ->
        flux.collectList().block()
    }
}

fun <RECORD> List<ConsumerRecord<PumpEvent, RECORD>>.toFluxBatch(
    convertToOperations: (PumpEvent, RECORD) -> Publisher<*>
): List<Flux<Any>> {

    if (this.isEmpty()) return emptyList()

    var previousEvent = PumpEvent.NEW_BLOCK
    val fluxesToExecute = mutableListOf<Flux<Any>>()

    var compiledFlux = Flux.empty<Any>()
    this.forEach { recordEvent ->
        val event = recordEvent.key()
        val record = recordEvent.value()
        val recordOperations = convertToOperations(event, record)

        val endBatch = (event != PumpEvent.NEW_BLOCK && previousEvent == PumpEvent.NEW_BLOCK)
            || (event == PumpEvent.NEW_BLOCK && previousEvent != PumpEvent.NEW_BLOCK)

        compiledFlux = if (endBatch) {
            fluxesToExecute.add(compiledFlux)
            Flux.empty<Any>().concatWith(recordOperations)
        } else {
            compiledFlux.concatWith(recordOperations)
        }

        previousEvent = event
    }

    fluxesToExecute.add(compiledFlux)

    return fluxesToExecute
}
