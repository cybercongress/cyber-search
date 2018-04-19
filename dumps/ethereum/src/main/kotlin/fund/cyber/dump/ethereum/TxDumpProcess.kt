package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumContractTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.dump.common.filterNotContainsAllEventsOf
import fund.cyber.dump.common.toRecordEventsMap
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

class TxDumpProcess(
        private val txRepository: EthereumTxRepository,
        private val blockTxRepository: EthereumBlockTxRepository,
        private val contractTxRepository: EthereumContractTxRepository,
        private val chain: EthereumFamilyChain,
        private val monitoring: MeterRegistry
) : BatchMessageListener<PumpEvent, EthereumTx> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    private lateinit var topicCurrentOffsetMonitor: AtomicLong


    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumTx>>) {

        log.info("Dumping batch of ${records.size} $chain txs from offset ${records.first().offset()}")

        val recordsToProcess = records.toRecordEventsMap()
                .filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK))

        val txsToCommit = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.NEW_BLOCK) }.keys
        val txsToRevert = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.DROPPED_BLOCK) }.keys

        txRepository.saveAll(txsToCommit.map { tx -> CqlEthereumTx(tx) }).collectList().block()
        txRepository.deleteAll(txsToRevert.map { tx -> CqlEthereumTx(tx) }).block()

        blockTxRepository.saveAll(txsToCommit.map { tx -> CqlEthereumBlockTxPreview(tx) }).collectList().block()
        blockTxRepository.deleteAll(txsToRevert.map { tx -> CqlEthereumBlockTxPreview(tx) }).block()

        val txsByAddressToSave = txsToCommit.flatMap { tx ->
            tx.addressesUsedInTransaction()
                    .map { it -> CqlEthereumContractTxPreview(tx, it) }
        }

        val txsByAddressToRevert = txsToRevert.flatMap { tx ->
            tx.addressesUsedInTransaction()
                    .map { it -> CqlEthereumContractTxPreview(tx, it) }
        }

        contractTxRepository.saveAll(txsByAddressToSave).collectList().block()
        contractTxRepository.deleteAll(txsByAddressToRevert).block()

        if (::topicCurrentOffsetMonitor.isInitialized) {
            topicCurrentOffsetMonitor.set(records.last().offset())
        } else {
            topicCurrentOffsetMonitor = monitoring.gauge("dump_topic_current_offset",
                    Tags.of("topic", chain.txPumpTopic), AtomicLong(records.last().offset()))!!
        }

    }
}
