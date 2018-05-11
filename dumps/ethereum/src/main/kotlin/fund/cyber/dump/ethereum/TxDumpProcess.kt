package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumBlockTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import java.util.concurrent.atomic.AtomicLong

/**
 * Transform transaction based on event. For [PumpEvent.DROPPED_BLOCK] we transform transaction to mempool state.
 * For others it remains as it is.
 *
 * @param event used to transform
 * @return transformed transaction
 */
fun EthereumTx.transformBy(event: PumpEvent) = if (event == PumpEvent.DROPPED_BLOCK) this.mempoolState() else this

class TxDumpProcess(
    private val txRepository: EthereumTxRepository,
    private val blockTxRepository: EthereumBlockTxRepository,
    private val contractTxRepository: EthereumContractTxRepository,
    private val chain: EthereumFamilyChain,
    monitoring: MeterRegistry
) : BatchMessageListener<PumpEvent, EthereumTx> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    private var topicCurrentOffsetMonitor: AtomicLong = monitoring.gauge("dump_topic_current_offset",
        Tags.of("topic", chain.txPumpTopic), AtomicLong(0L))!!


    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumTx>>) {

        log.info("Dumping batch of ${records.size} $chain transactions from offset ${records.first().offset()}")

        val txLastEventMap = recordIdToLastEvent(records)

        val currentDbTxs = txRepository.findAllById(records.map { r -> r.value().hash }).collectList().block()!!
            .associateBy { tx -> tx.hash }

        val txsToSave = txLastEventMap
            // we don't need to save pool tx if we already have some state for this tx in DB
            .filter { entry -> !(entry.value.first == PumpEvent.NEW_POOL_TX && currentDbTxs.contains(entry.key)) }
            .map { entry ->
                val (event, tx) = entry.value
                val currentDbTx = currentDbTxs[entry.value.second.hash]
                val newTx = tx.transformBy(event)
                // Set first seen time from previous state in DB
                if (currentDbTx != null) {
                    return@map newTx.copy(firstSeenTime = currentDbTx.firstSeenTime)
                }
                return@map newTx
            }

        log.info("Total transactions to save ${txsToSave.size}")

        txRepository.saveAll(txsToSave.map { tx -> CqlEthereumTx(tx) }).collectList().block()
        blockTxRepository.saveAll(txsToSave.map { tx -> CqlEthereumBlockTxPreview(tx) }).collectList().block()

        val txsByContractHash = txsToSave.flatMap { tx ->
            tx.contractsUsedInTransaction().map { it -> CqlEthereumContractTxPreview(tx, it) }
        }

        contractTxRepository.saveAll(txsByContractHash).collectList().block()

        topicCurrentOffsetMonitor.set(records.last().offset())
    }

    /**
     * Create map of record id to last applied event and state to handle situation when we have multiply records
     * in batch with the same id. It could happen when they have different events.
     *
     * @param records list of records
     * @return map of record id to last applied event and record state
     *
     */
    private fun recordIdToLastEvent(
        records: List<ConsumerRecord<PumpEvent, EthereumTx>>
    ): MutableMap<String, Pair<PumpEvent, EthereumTx>> {
        val txLastEventMap = mutableMapOf<String, Pair<PumpEvent, EthereumTx>>()
        records.forEach { record ->
            val event = record.key()
            val tx = record.value()
            val currentTxState = txLastEventMap[tx.hash]
            if (currentTxState == null) {
                txLastEventMap[tx.hash] = event to tx
            } else {
                if (record.key() != PumpEvent.NEW_POOL_TX) {
                    txLastEventMap[tx.hash] = event to tx
                }
            }
        }
        return txLastEventMap
    }

}
