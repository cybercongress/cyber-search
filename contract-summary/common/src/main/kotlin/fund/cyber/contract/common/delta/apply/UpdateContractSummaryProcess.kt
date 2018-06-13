package fund.cyber.contract.common.delta.apply

import fund.cyber.cassandra.common.CqlContractSummary
import fund.cyber.common.kafka.reader.SinglePartitionTopicLastItemsReader
import fund.cyber.contract.common.delta.ContractSummaryDelta
import fund.cyber.contract.common.delta.DeltaMerger
import fund.cyber.contract.common.delta.DeltaProcessor
import fund.cyber.contract.common.summary.ContractSummaryStorage
import fund.cyber.search.model.events.PumpEvent
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.scheduler.Schedulers


private fun <T> Flux<T>.await() = this.collectList().block()!!

fun CqlContractSummary.hasSameTopicPartitionAs(delta: ContractSummaryDelta<*>) =
    this.kafkaDeltaTopic == delta.topic && this.kafkaDeltaPartition == delta.partition

fun CqlContractSummary.hasSameTopicPartitionAs(topic: String, partition: Int) =
    this.kafkaDeltaTopic == topic && this.kafkaDeltaPartition == partition

fun CqlContractSummary.notSameTopicPartitionAs(delta: ContractSummaryDelta<*>) =
    hasSameTopicPartitionAs(delta).not()

fun CqlContractSummary.committed() = this.kafkaDeltaOffsetCommitted

fun CqlContractSummary.notCommitted() = committed().not()

@Suppress("MagicNumber")
private val storeDeltasThreadPool = Schedulers.newParallel("store-deltas", 8)
private val log = LoggerFactory.getLogger(UpdateContractSummaryProcess::class.java)!!

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
class UpdateContractSummaryProcess<R, S : CqlContractSummary, D : ContractSummaryDelta<S>>(
    private val contractSummaryStorage: ContractSummaryStorage<S>,
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
    private lateinit var recordsProcessingTimer: Timer

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, R>>, consumer: Consumer<*, *>) {
        val info = UpdateInfo(records.sortedBy { record -> record.offset() })
        initMonitors(info)
        recordsProcessingTimer.recordCallable { processRecords(records, consumer, info) }
    }

    private fun processRecords(records: List<ConsumerRecord<PumpEvent, R>>, consumer: Consumer<*, *>,
                               info: UpdateInfo) {

        log.info("Processing records for topic: ${info.topic}; partition ${info.partition} from ${info.minOffset}" +
            " to ${info.maxOffset} offset.")

        val recordsToProcess = records.filter { record -> record.key() != PumpEvent.NEW_POOL_TX }

        val storeAttempts: MutableMap<String, Int> = mutableMapOf()
        val previousStates: MutableMap<String, S?> = mutableMapOf()

        val contracts = deltaProcessor.affectedContracts(recordsToProcess)

        val contractsSummary = contractSummaryStorage.findAllByIdIn(contracts)
            .await().groupBy { a -> a.hash }.map { (k, v) -> k to v.first() }.toMap()

        val deltas = recordsToProcess.flatMap { record -> deltaProcessor.recordToDeltas(record) }

        val mergedDeltas = deltas.groupBy { delta -> delta.contract }
            .filterKeys { contract -> contract.isNotEmpty() }
            .mapValues { contractDeltas -> deltaMerger.mergeDeltas(contractDeltas.value, contractsSummary) }
            .filterValues { value -> value != null }
            .map { entry -> entry.key to entry.value!! }.toMap()

        try {

            deltaStoreTimer.recordCallable {
                mergedDeltas.values.toFlux().flatMap { delta ->
                    Mono.fromCallable { store(contractsSummary[delta.contract], delta, storeAttempts, previousStates) }
                        .subscribeOn(storeDeltasThreadPool)
                }.await()
            }

            commitKafkaTimer.recordCallable { consumer.commitSync() }

            val newSummaries = downloadCassandraTimer.recordCallable {
                contracts.toFlux().flatMap { contractId -> contractSummaryStorage.findById(contractId) }.await()
            }

            commitCassandraTimer.recordCallable {

                newSummaries.toFlux().flatMap { summary ->
                    contractSummaryStorage.commitUpdate(summary.hash, summary.version + 1)
                }.await()
            }
        } catch (e: ContractLockException) {

            log.debug("Possible contract lock for ${info.topic} topic," +
                " ${info.partition} partition, offset: ${info.minOffset}-${info.maxOffset}. Reverting changes...")
            applyLockMonitor.increment()
            revertChanges(contracts, previousStates, info)
            log.debug("Changes for ${info.topic} topic, ${info.partition} partition," +
                " offset: ${info.minOffset}-${info.maxOffset} reverted!")
        }
    }

    private fun revertChanges(contracts: Set<String>, previousStates: MutableMap<String, S?>, info: UpdateInfo) {

        contractSummaryStorage.findAllByIdIn(contracts).await().forEach { summary ->
            if (summary.notCommitted() && summary.hasSameTopicPartitionAs(info.topic, info.partition)
                && summary.kafkaDeltaOffset in info.minOffset..info.maxOffset) {
                val previousState = previousStates[summary.hash]
                if (previousState != null) {
                    contractSummaryStorage.update(previousState)
                } else {
                    contractSummaryStorage.remove(summary.hash)
                }
            }
        }
    }

    @Suppress("ComplexMethod", "NestedBlockDepth")
    private fun store(summary: S?, delta: D, storeAttempts: MutableMap<String, Int>,
                      previousStates: MutableMap<String, S?>) {

        previousStates[delta.contract] = summary
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
                if (storeAttempts[delta.contract] ?: 0 > MAX_STORE_ATTEMPTS) {
                    if (summary.currentTopicPartitionWentFurther()) {
                        val result = delta.applyTo(summary)
                        if (!result) {
                            storeAttempts[delta.contract] = 0
                            store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
                        }
                    } else {
                        throw ContractLockException()
                    }
                } else {
                    Thread.sleep(STORE_RETRY_TIMEOUT)
                    val inc = storeAttempts.getOrPut(delta.contract, { 1 }).inc()
                    storeAttempts[delta.contract] = inc
                    store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
                }
            }
        } else {
            val newSummary = delta.createSummary()
            val result = contractSummaryStorage.insertIfNotRecord(newSummary).block()!!
            if (!result) {
                store(getSummaryByDelta(delta), delta, storeAttempts, previousStates)
            }
        }
    }

    private fun D.applyTo(summary: S): Boolean =
        contractSummaryStorage.update(this.updateSummary(summary), summary.version).block()!!

    private fun getSummaryByDelta(delta: D) = contractSummaryStorage.findById(delta.contract).block()!!

    private fun initMonitors(info: UpdateInfo) {
        val tags = Tags.of("topic", info.topic)
        if (!(::applyLockMonitor.isInitialized)) {
            applyLockMonitor = monitoring.counter("contract_summary_apply_lock_counter", tags)
        }
        if (!(::deltaStoreTimer.isInitialized)) {
            deltaStoreTimer = monitoring.timer("contract_summary_delta_store", tags)
        }
        if (!(::commitKafkaTimer.isInitialized)) {
            commitKafkaTimer = monitoring.timer("contract_summary_commit_kafka", tags)
        }
        if (!(::commitCassandraTimer.isInitialized)) {
            commitCassandraTimer = monitoring.timer("contract_summary_commit_cassandra", tags)
        }
        if (!(::downloadCassandraTimer.isInitialized)) {
            downloadCassandraTimer = monitoring.timer("contract_summary_download_cassandra", tags)
        }
        if (!(::recordsProcessingTimer.isInitialized)) {
            recordsProcessingTimer = monitoring.timer("contract_summary_records_processing", tags)
        }
    }

    private fun CqlContractSummary.currentTopicPartitionWentFurther() =
        lastOffsetOf(this.kafkaDeltaTopic, this.kafkaDeltaPartition) > this.kafkaDeltaOffset//todo : = or >= ????

    private fun lastOffsetOf(topic: String, partition: Int): Long {

        val reader = SinglePartitionTopicLastItemsReader(
            kafkaBrokers = kafkaBrokers, topic = topic,
            keyClass = Any::class.java, valueClass = Any::class.java
        )
        return reader.readLastOffset(partition)
    }

}
