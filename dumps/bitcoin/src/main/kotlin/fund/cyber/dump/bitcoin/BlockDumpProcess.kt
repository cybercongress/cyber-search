package fund.cyber.dump.bitcoin

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.dump.common.filterNotContainsAllEventsOf
import fund.cyber.dump.common.toRecordEventsMap
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import java.util.concurrent.atomic.AtomicLong


class BlockDumpProcess(
        private val blockRepository: BitcoinBlockRepository,
        private val chain: BitcoinFamilyChain,
        private val monitoring: MeterRegistry
) : BatchMessageListener<PumpEvent, BitcoinBlock> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    private lateinit var topicCurrentOffsetMonitor: AtomicLong

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, BitcoinBlock>>) {

        val first = records.first()
        val last = records.last()
        log.info("Dumping batch of ${first.value().height}-${last.value().height} $chain blocks")

        val recordsToProcess = records.toRecordEventsMap()
                .filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK))

        val blocksToCommit = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.NEW_BLOCK) }.keys
        val blocksToRevert = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.DROPPED_BLOCK) }.keys

        blockRepository
                .saveAll(blocksToCommit.map { block -> CqlBitcoinBlock(block) })
                .collectList().block()
        blockRepository.deleteAll(blocksToRevert.map { block -> CqlBitcoinBlock(block) })
                .block()

        if (::topicCurrentOffsetMonitor.isInitialized) {
            topicCurrentOffsetMonitor.set(records.last().offset())
        } else {
            topicCurrentOffsetMonitor = monitoring.gauge("dump_topic_current_offset",
                    Tags.of("topic", chain.blockPumpTopic), AtomicLong(records.last().offset()))!!
        }
    }
}
