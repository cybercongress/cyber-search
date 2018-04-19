package fund.cyber.dump.bitcoin

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlockTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.dump.common.filterNotContainsAllEventsOf
import fund.cyber.dump.common.toRecordEventsMap
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import java.util.concurrent.atomic.AtomicLong

class TxDumpProcess(
        private val txRepository: BitcoinTxRepository,
        private val contractTxRepository: BitcoinContractTxRepository,
        private val blockTxRepository: BitcoinBlockTxRepository,
        private val chain: BitcoinFamilyChain,
        private val monitoring: MeterRegistry
) : BatchMessageListener<PumpEvent, BitcoinTx> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    private lateinit var topicCurrentOffsetMonitor: AtomicLong


    override fun onMessage(records: List<ConsumerRecord<PumpEvent, BitcoinTx>>) {

        log.info("Dumping batch of ${records.size} $chain txs from offset ${records.first().offset()}")

        val recordsToProcess = records.toRecordEventsMap()
                .filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK))

        val txsToCommit = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.NEW_BLOCK) }.keys
        val txsToRevert = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.DROPPED_BLOCK) }.keys

        txRepository
                .saveAll(txsToCommit.map { tx -> CqlBitcoinTx(tx) })
                .collectList().block()
        txRepository.deleteAll(txsToRevert.map { tx -> CqlBitcoinTx(tx) })
                .block()

        contractTxRepository
                .saveAll(txsToCommit.flatMap { tx ->
                    tx.allAddressesUsedInTransaction().map { address -> CqlBitcoinContractTxPreview(address, tx) }
                })
                .collectList().block()
        contractTxRepository
                .deleteAll(txsToRevert.flatMap { tx ->
                    tx.allAddressesUsedInTransaction().map { address -> CqlBitcoinContractTxPreview(address, tx) }
                })
                .block()

        blockTxRepository
                .saveAll(txsToCommit.map { tx -> CqlBitcoinBlockTxPreview(tx) })
                .collectList().block()
        blockTxRepository.deleteAll(txsToRevert.map { tx -> CqlBitcoinBlockTxPreview(tx) })
                .block()
        if (::topicCurrentOffsetMonitor.isInitialized) {
            topicCurrentOffsetMonitor.set(records.last().offset())
        } else {
            topicCurrentOffsetMonitor = monitoring.gauge("dump_topic_current_offset",
                    Tags.of("topic", chain.txPumpTopic), AtomicLong(records.last().offset()))!!
        }
    }
}
