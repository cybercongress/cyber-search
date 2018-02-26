package fund.cyber.address.common.delta.apply

import fund.cyber.address.common.delta.AddressSummaryDelta
import fund.cyber.address.common.delta.DeltaMerger
import fund.cyber.address.common.delta.DeltaProcessor
import fund.cyber.address.common.summary.AddressSummaryStorage
import fund.cyber.cassandra.common.CqlAddressSummary
import fund.cyber.common.kafka.reader.SinglePartitionTopicLastItemsReader
import fund.cyber.search.model.events.PumpEvent
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener
import reactor.core.publisher.Flux
import java.util.concurrent.atomic.AtomicLong

fun <T> Flux<T>.await(): List<T> {
    return this.collectList().block()!!
}

private val log = LoggerFactory.getLogger(UpdateAddressSummaryProcess::class.java)!!

/**
 *
 * This process should not be aware of chain reorganisation
 *
 * */
//todo add tests
//todo add deadlock catcher
class UpdateAddressSummaryProcess<R, S : CqlAddressSummary, D : AddressSummaryDelta<S>>(
        private val addressSummaryStorage: AddressSummaryStorage<S>,
        private val deltaProcessor: DeltaProcessor<R, S, D>,
        private val deltaMerger: DeltaMerger<D>,
        private val monitoring: MeterRegistry,
        private val kafkaBrokers: String
) : BatchConsumerAwareMessageListener<PumpEvent, R> {

    private lateinit var topicCurrentOffsetMonitor: AtomicLong
    private lateinit var applyLockMonitor: Counter

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, R>>, consumer: Consumer<*, *>) {
        val topic = records.first().topic()
        val partition = records.first().partition()
        val storeAttempts: MutableMap<String, Int> = mutableMapOf()
        val previousStates: MutableMap<String, S?> = mutableMapOf()
        initMonitors(records.first())

        val addresses = deltaProcessor.affectedAddresses(records)

        val addressesSummary = addressSummaryStorage.findAllByIdIn(addresses)
                .await().groupBy { a -> a.id }.map { (k, v) -> k to v.first() }.toMap()

        val deltas = records.flatMap { record -> deltaProcessor.recordToDeltas(record) }

        val mergedDeltas = deltas.groupBy { delta -> delta.address }
                .filterKeys { address -> address.isNotEmpty() }
                .mapValues { addressDeltas -> deltaMerger.mergeDeltas(addressDeltas.value, addressesSummary) }
                .filterValues { value -> value != null }
                .map { entry -> entry.key to entry.value!! }.toMap()

        try {
            mergedDeltas.values.forEach { delta -> store(addressesSummary[delta.address], delta, storeAttempts, previousStates) }

            consumer.commitSync()

            val newSummaries = addressSummaryStorage.findAllByIdIn(addresses).await()

            newSummaries.forEach { summary ->
                addressSummaryStorage.commitUpdate(summary.id, summary.version + 1).block()
            }

            monitoring.gauge("address_summary_topic_current_offset", Tags.of("topic", records.last().topic()),
                    records.last().offset())!!
        } catch (e: AddressLockException) {
            val sortedByOffset = records.sortedBy { record -> record.offset() }
            val minOffset = sortedByOffset.first().offset()
            val maxOffset = sortedByOffset.last().offset()
            log.error("Possible address lock for ${records.first().topic()} topic," +
                    " ${records.first().partition()} partition, offset: $minOffset-$maxOffset. Reverting changes...")
            applyLockMonitor.increment()
            addressSummaryStorage.findAllByIdIn(addresses).await().forEach { summary ->
                if (summary.notCommitted() && summary.hasSameTopicPartitionAs(topic, partition)
                        && summary.kafkaDeltaOffset in minOffset..maxOffset) {
                    // should revert to previous state
                    val previousState = previousStates[summary.id]
                    if (previousState != null) {
                        addressSummaryStorage.update(previousState)
                    } else {
                        addressSummaryStorage.remove(summary.id)
                    }
                }
            }
            log.error("Changes for ${records.first().topic()} topic, ${records.first().partition()} partition," +
                    " offset: $minOffset-$maxOffset reverted!")
        }

    }

    private fun store(addressSummary: S?, delta: D, storeAttempts: MutableMap<String, Int>, previousStates: MutableMap<String, S?>) {
        previousStates[delta.address] = addressSummary
        if (addressSummary != null) {
            if (addressSummary.committed()) {
                val result = delta.applyTo(addressSummary)
                if (!result) {
                    store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
                }
            }

            if (addressSummary.notCommitted() && addressSummary.hasSameTopicPartitionAs(delta)) {
                delta.applyTo(addressSummary)
            }

            if (addressSummary.notCommitted() && addressSummary.notSameTopicPartionAs(delta)) {
                //todo: timeouts???
                if (storeAttempts[delta.address] ?: 0 > 5) {
                    val reader = SinglePartitionTopicLastItemsReader(kafkaBrokers, addressSummary.kafkaDeltaTopic,
                            Any::class.java, Any::class.java)
                    val lastOffset = reader.readLastOffset(addressSummary.kafkaDeltaPartition)
                    //todo : = or >= ????
                    if (lastOffset >= addressSummary.kafkaDeltaOffset) {
                        val result = delta.applyTo(addressSummary)
                        if (!result) {
                            storeAttempts[delta.address] = 0
                            store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
                        }
                    } else {
                        throw AddressLockException()
                    }
                } else {
                    storeAttempts.getOrPut(delta.address, { 1 }).inc()
                    store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
                }
            }
        } else {
            val summary = delta.createSummary()
            val result = addressSummaryStorage.insertIfNotRecord(summary).block()!!
            if (!result) {
                store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
            }
        }
    }

    private fun D.applyTo(summary: S): Boolean =
            addressSummaryStorage.update(this.updateSummary(summary), summary.version).block()!!

    private fun getSummaryByDelta(delta: D) =
            addressSummaryStorage.findById(delta.address).block()!!

    private fun CqlAddressSummary.hasSameTopicPartitionAs(delta: D) =
            this.kafkaDeltaTopic == delta.topic && this.kafkaDeltaPartition == delta.partition

    private fun CqlAddressSummary.hasSameTopicPartitionAs(topic: String, partition: Int) =
            this.kafkaDeltaTopic == topic && this.kafkaDeltaPartition == partition

    private fun CqlAddressSummary.notSameTopicPartionAs(delta: D) =
            hasSameTopicPartitionAs(delta).not()

    private fun CqlAddressSummary.committed() = this.kafkaDeltaOffsetCommitted

    private fun CqlAddressSummary.notCommitted() = committed().not()

    private fun initMonitors(record: ConsumerRecord<PumpEvent, R>) {
        if (!(::topicCurrentOffsetMonitor.isInitialized)) {
            topicCurrentOffsetMonitor = monitoring.gauge("address_summary_topic_current_offset",
                    Tags.of("topic", record.topic()), AtomicLong(record.offset()))!!
        }
        if (!(::applyLockMonitor.isInitialized)) {
            applyLockMonitor = monitoring.counter("address_summary_apply_lock_counter", Tags.of("topic", record.topic()))
        }
    }

}