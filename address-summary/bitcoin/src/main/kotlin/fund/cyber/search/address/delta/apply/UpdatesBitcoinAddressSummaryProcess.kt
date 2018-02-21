package fund.cyber.search.address.delta.apply


import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressSummary
import fund.cyber.cassandra.bitcoin.repository.BitcoinUpdateAddressSummaryRepository
import fund.cyber.search.address.summary.BitcoinAddressSummaryDelta
import fund.cyber.search.address.summary.getAffectedAddresses
import fund.cyber.search.address.summary.mergeDeltas
import fund.cyber.search.address.summary.txToDeltas
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

fun <T> Flux<T>.await(): List<T> {
    return this.collectList().block()!!
}

private fun CqlBitcoinAddressSummary.hasSameTopicPartitionAs(delta: BitcoinAddressSummaryDelta) =
        this.kafka_delta_topic == delta.topic && this.kafka_delta_partition == delta.partition

private fun CqlBitcoinAddressSummary.notSameTopicPartionAs(delta: BitcoinAddressSummaryDelta) =
        hasSameTopicPartitionAs(delta).not()

private fun CqlBitcoinAddressSummary.committed() = this.kafka_delta_offset_committed

private fun CqlBitcoinAddressSummary.notCommitted() = committed().not()

/**
 *
 * This process should not be aware of chain reorganisation
 *
 * */
//todo add tests
//todo add deadlock catcher
@Component
class UpdatesBitcoinAddressSummaryProcess(
        private val addressSummaryRepository: BitcoinUpdateAddressSummaryRepository
) : BatchConsumerAwareMessageListener<PumpEvent, BitcoinTx> {


    override fun onMessage(records: List<ConsumerRecord<PumpEvent, BitcoinTx>>, consumer: Consumer<*, *>) {
        val addresses = getAffectedAddresses(records)

        val addressesSummary = addressSummaryRepository.findAllByIdIn(addresses)
                .await().groupBy { a -> a.id }.map { (k, v) -> k to v.first() }.toMap()

        val deltas = records.flatMap { record -> txToDeltas(record) }

        val mergedDeltas = deltas.groupBy { delta -> delta.address }
                .mapValues { addressDeltas -> addressDeltas.value.mergeDeltas(addressesSummary) }
                .filterValues { value -> value != null }
                .map { entry -> entry.key to entry.value!! }.toMap()

        mergedDeltas.values.parallelStream().forEach { delta ->
            store(addressesSummary[delta.address], delta)
        }

        consumer.commitSync()

        val newSummaries = addressSummaryRepository.findAllByIdIn(addresses).await()
        newSummaries.forEach{ summary -> addressSummaryRepository.commitUpdate(summary.id, summary.version + 1).block() } //todo: blocking operation will be executed one by one!!!!
    }

    private fun store(addressSummary: CqlBitcoinAddressSummary?, delta: BitcoinAddressSummaryDelta) {
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
            val summary = createFromDelta(delta)
            val result = addressSummaryRepository.insertIfNotExists(summary).block()!!
            if (!result) {
                store(getSummaryByDelta(delta), delta)
            }
        }
    }

    private fun BitcoinAddressSummaryDelta.applyTo(summary: CqlBitcoinAddressSummary): Boolean =
            addressSummaryRepository.update(summary.updateFromDelta(this), summary.version).block()!!

    private fun getSummaryByDelta(delta: BitcoinAddressSummaryDelta) =
            addressSummaryRepository.findById(delta.address).block()!!

    private fun createFromDelta(delta: BitcoinAddressSummaryDelta): CqlBitcoinAddressSummary {
        return CqlBitcoinAddressSummary(
                id = delta.address, confirmed_balance = delta.balanceDelta,
                confirmed_tx_number = delta.txNumberDelta,
                confirmed_total_received = delta.totalReceivedDelta,
                kafka_delta_offset = delta.offset, kafka_delta_topic = delta.topic,
                kafka_delta_partition = delta.partition, version = 0
        )
    }

    private fun CqlBitcoinAddressSummary.updateFromDelta(delta: BitcoinAddressSummaryDelta): CqlBitcoinAddressSummary {
        return CqlBitcoinAddressSummary(
                id = this.id, confirmed_balance = this.confirmed_balance + delta.balanceDelta,
                confirmed_tx_number = this.confirmed_tx_number + delta.txNumberDelta,
                confirmed_total_received = this.confirmed_total_received + delta.totalReceivedDelta,
                kafka_delta_offset = delta.offset, kafka_delta_topic = delta.topic,
                kafka_delta_partition = delta.partition, version = this.version + 1
        )
    }

}