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
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.runBlocking
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.suspendCoroutine

fun <T> Flux<T>.await(): List<T> {
    return this.collectList().block()!!
}

fun CqlAddressSummary.hasSameTopicPartitionAs(delta: AddressSummaryDelta<*>) =
        this.kafkaDeltaTopic == delta.topic && this.kafkaDeltaPartition == delta.partition

fun CqlAddressSummary.hasSameTopicPartitionAs(topic: String, partition: Int) =
        this.kafkaDeltaTopic == topic && this.kafkaDeltaPartition == partition

fun CqlAddressSummary.notSameTopicPartitionAs(delta: AddressSummaryDelta<*>) =
        hasSameTopicPartitionAs(delta).not()

fun CqlAddressSummary.committed() = this.kafkaDeltaOffsetCommitted

fun CqlAddressSummary.notCommitted() = committed().not()

@Suppress("MagicNumber")
private val applicationContext = newFixedThreadPoolContext(8, "Coroutines Concurrent Pool")
private val log = LoggerFactory.getLogger(UpdateAddressSummaryProcess::class.java)!!

private const val MAX_STORE_ATTEMPTS = 20
private const val STORE_RETRY_TIMEOUT = 30L

data class UpdateInfo(
        val topic: String,
        val partition: Int,
        val minOffset: Long,
        val maxOffset: Long
) {
    constructor(records: List<ConsumerRecord<*, *>>) : this(
            topic = records.first().topic(), partition = records.first().partition(),
            minOffset = records.first().offset(), maxOffset = records.last().offset()
    )
}

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

    private lateinit var applyLockMonitor: Counter
    private lateinit var deltaStoreTimer: Timer
    private lateinit var commitKafkaTimer: Timer
    private lateinit var commitCassandraTimer: Timer
    private lateinit var downloadCassandraTimer: Timer
    private lateinit var currentOffsetMonitor: AtomicLong

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, R>>, consumer: Consumer<*, *>) {

        val info = UpdateInfo(records.sortedBy { record -> record.offset() })
        val storeAttempts: MutableMap<String, Int> = mutableMapOf()
        val previousStates: MutableMap<String, S?> = mutableMapOf()
        initMonitors(info)

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

            deltaStoreTimer.recordCallable {
                runBlocking {
                    mergedDeltas.values.map { delta ->
                        async(applicationContext) {
                            store(addressesSummary[delta.address], delta, storeAttempts, previousStates)
                        }
                    }.map { it.await() }
                }
            }

            commitKafkaTimer.recordCallable { consumer.commitSync() }

            val newSummaries = downloadCassandraTimer.recordCallable {
                addressSummaryStorage.findAllByIdIn(addresses).await()
            }

            commitCassandraTimer.recordCallable {
                runBlocking {
                    newSummaries.map { summary ->
                        async(applicationContext) {
                            addressSummaryStorage.commitUpdate(summary.id, summary.version + 1).await()
                        }
                    }.map { it.await() }
                }
            }

            currentOffsetMonitor.set(info.maxOffset)

        } catch (e: AddressLockException) {

            log.debug("Possible address lock for ${info.topic} topic," +
                    " ${info.partition} partition, offset: ${info.minOffset}-${info.maxOffset}. Reverting changes...")
            applyLockMonitor.increment()
            revertChanges(addresses, previousStates, info)
            log.debug("Changes for ${info.topic} topic, ${info.partition} partition," +
                    " offset: ${info.minOffset}-${info.maxOffset} reverted!")
        }
    }

    private fun revertChanges(addresses: Set<String>, previousStates: MutableMap<String, S?>, info: UpdateInfo) {

        addressSummaryStorage.findAllByIdIn(addresses).await().forEach { summary ->
            if (summary.notCommitted() && summary.hasSameTopicPartitionAs(info.topic, info.partition)
                    && summary.kafkaDeltaOffset in info.minOffset..info.maxOffset) {
                val previousState = previousStates[summary.id]
                if (previousState != null) {
                    addressSummaryStorage.update(previousState)
                } else {
                    addressSummaryStorage.remove(summary.id)
                }
            }
        }
    }

    @Suppress("ComplexMethod", "NestedBlockDepth")
    private suspend fun store(summary: S?, delta: D, storeAttempts: MutableMap<String, Int>,
                              previousStates: MutableMap<String, S?>) {

        previousStates[delta.address] = summary
        if (summary != null) {
            if (summary.committed()) {
                val result = delta.applyTo(summary)
                if (!result) {
                    store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
                }
            }

            if (summary.notCommitted() && summary.hasSameTopicPartitionAs(delta)) {
                delta.applyTo(summary)
            }

            if (summary.notCommitted() && summary.notSameTopicPartitionAs(delta)) {
                if (storeAttempts[delta.address] ?: 0 > MAX_STORE_ATTEMPTS) {
                    if (summary.currentTopicPartitionWentFurther()) {
                        val result = delta.applyTo(summary)
                        if (!result) {
                            storeAttempts[delta.address] = 0
                            store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
                        }
                    } else {
                        throw AddressLockException()
                    }
                } else {
                    delay(STORE_RETRY_TIMEOUT, TimeUnit.MILLISECONDS)
                    val inc = storeAttempts.getOrPut(delta.address, { 1 }).inc()
                    storeAttempts[delta.address] = inc
                    store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
                }
            }
        } else {
            val newSummary = delta.createSummary()
            val result = addressSummaryStorage.insertIfNotRecord(newSummary).block()!!
            if (!result) {
                store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
            }
        }
    }

    private suspend fun D.applyTo(summary: S): Boolean =
            addressSummaryStorage.update(this.updateSummary(summary), summary.version).await()!!

    private suspend fun getSummaryByDelta(delta: D) = addressSummaryStorage.findById(delta.address).await()

    private fun initMonitors(info: UpdateInfo) {
        val tags = Tags.of("topic", info.topic)
        if (!(::applyLockMonitor.isInitialized)) {
            applyLockMonitor = monitoring.counter("address_summary_apply_lock_counter", tags)
        }
        if (!(::deltaStoreTimer.isInitialized)) {
            deltaStoreTimer = monitoring.timer("address_summary_delta_store", tags)
        }
        if (!(::commitKafkaTimer.isInitialized)) {
            commitKafkaTimer = monitoring.timer("address_summary_commit_kafka", tags)
        }
        if (!(::commitCassandraTimer.isInitialized)) {
            commitCassandraTimer = monitoring.timer("address_summary_commit_cassandra", tags)
        }
        if (!(::downloadCassandraTimer.isInitialized)) {
            downloadCassandraTimer = monitoring.timer("address_summary_download_cassandra", tags)
        }
        if (!(::currentOffsetMonitor.isInitialized)) {
            currentOffsetMonitor = monitoring.gauge("address_summary_topic_current_offset", tags,
                    AtomicLong(info.maxOffset))!!
        }
    }

    private fun CqlAddressSummary.currentTopicPartitionWentFurther() =
            lastOffsetOf(this.kafkaDeltaTopic, this.kafkaDeltaPartition) > this.kafkaDeltaOffset//todo : = or >= ????

    private fun lastOffsetOf(topic: String, partition: Int): Long {

        val reader = SinglePartitionTopicLastItemsReader(
                kafkaBrokers = kafkaBrokers, topic = topic,
                keyClass = Any::class.java, valueClass = Any::class.java
        )
        return reader.readLastOffset(partition)
    }

    private suspend fun <T> Mono<T>.await(): T? {
        return suspendCoroutine { cont: Continuation<T> ->
            doOnSuccessOrError { data, error ->
                if (error == null) cont.resume(data) else cont.resumeWithException(error)
            }.subscribe()
        }
    }
}
