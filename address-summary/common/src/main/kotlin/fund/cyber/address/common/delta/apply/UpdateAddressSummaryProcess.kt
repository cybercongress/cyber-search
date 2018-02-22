package fund.cyber.address.common.delta.apply

import fund.cyber.address.common.delta.AddressSummaryDelta
import fund.cyber.address.common.delta.DeltaMerger
import fund.cyber.address.common.delta.DeltaProcessor
import fund.cyber.address.common.summary.AddressSummaryStorage
import fund.cyber.cassandra.common.CqlAddressSummary
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener
import reactor.core.publisher.Flux

fun <T> Flux<T>.await(): List<T> {
    return this.collectList().block()!!
}

/**
 *
 * This process should not be aware of chain reorganisation
 *
 * */
//todo add tests
//todo add deadlock catcher
class UpdatesAddressSummaryProcess<R, S : CqlAddressSummary, D : AddressSummaryDelta<S>>(
        private val addressSummaryStorage: AddressSummaryStorage<S>,
        private val deltaProcessor: DeltaProcessor<R, S, D>,
        private val deltaMerger: DeltaMerger<D>
) : BatchConsumerAwareMessageListener<PumpEvent, R> {


    override fun onMessage(records: List<ConsumerRecord<PumpEvent, R>>, consumer: Consumer<*, *>) {
        val addresses = deltaProcessor.affectedAddresses(records)

        val addressesSummary = addressSummaryStorage.findAllByIdIn(addresses)
                .await().groupBy { a -> a.id }.map { (k, v) -> k to v.first() }.toMap()

        val deltas = records.flatMap { record -> deltaProcessor.recordToDeltas(record) }

        val mergedDeltas = deltas.groupBy { delta -> delta.address }
                .mapValues { addressDeltas -> deltaMerger.mergeDeltas(addressDeltas.value, addressesSummary) }
                .filterValues { value -> value != null }
                .map { entry -> entry.key to entry.value!! }.toMap()

        //todo: remove parallelStream()
        mergedDeltas.values.forEach { delta ->
            store(addressesSummary[delta.address], delta)
        }

        consumer.commitSync()

        val newSummaries = addressSummaryStorage.findAllByIdIn(addresses).await()

        //todo: blocking operation will be executed one by one!!!!
        newSummaries.forEach { summary -> addressSummaryStorage.commitUpdate(summary.id, summary.version + 1).block() }
    }

    private fun store(addressSummary: S?, delta: D) {
        if (addressSummary != null) {
            if (addressSummary.committed()) {
                val result = delta.applyTo(addressSummary)
                if (!result) {
                    store(getSummaryByDelta(delta), delta)
                }
            }

            if (addressSummary.notCommitted() && addressSummary.hasSameTopicPartitionAs(delta)) {
                delta.applyTo(addressSummary)
            }

            if (addressSummary.notCommitted() && addressSummary.notSameTopicPartionAs(delta)) {
                val result = delta.applyTo(getSummaryByDelta(delta))
                if (!result) {
                    store(getSummaryByDelta(delta), delta)
                }
            }
        } else {
            val summary = delta.createSummary()
            val result = addressSummaryStorage.insertIfNotRecord(summary).block()!!
            if (!result) {
                store(getSummaryByDelta(delta), delta)
            }
        }
    }

    private fun D.applyTo(summary: S): Boolean =
            addressSummaryStorage.update(this.updateSummary(summary), summary.version).block()!!

    private fun getSummaryByDelta(delta: D) =
            addressSummaryStorage.findById(delta.address).block()!!

    private fun CqlAddressSummary.hasSameTopicPartitionAs(delta: D) =
            this.kafka_delta_topic == delta.topic && this.kafka_delta_partition == delta.partition

    private fun CqlAddressSummary.notSameTopicPartionAs(delta: D) =
            hasSameTopicPartitionAs(delta).not()

    private fun CqlAddressSummary.committed() = this.kafka_delta_offset_committed

    private fun CqlAddressSummary.notCommitted() = committed().not()
}