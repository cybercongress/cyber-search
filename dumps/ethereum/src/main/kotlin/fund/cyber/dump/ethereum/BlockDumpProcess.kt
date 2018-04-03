package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.repository.EthereumAddressMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import java.util.concurrent.atomic.AtomicLong


class BlockDumpProcess(
        private val blockRepository: EthereumBlockRepository,
        private val addressMinedBlockRepository: EthereumAddressMinedBlockRepository,
        private val chain: EthereumFamilyChain,
        private val monitoring: MeterRegistry
) : BatchMessageListener<PumpEvent, EthereumBlock> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    private lateinit var topicCurrentOffsetMonitor: AtomicLong


    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumBlock>>) {

        val first = records.first()
        val last = records.last()
        log.info("Dumping batch of ${first.value().number}-${last.value().number} $chain blocks")

        val recordsToProcess = records.toRecordEventsMap()
                .filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK))

        val blocksToCommit = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.NEW_BLOCK) }.keys
        val blocksToRevert = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.DROPPED_BLOCK) }.keys

        blockRepository
                .saveAll(blocksToCommit.map { block -> CqlEthereumBlock(block) })
                .collectList().block()
        blockRepository.deleteAll(blocksToRevert.map { block -> CqlEthereumBlock(block) })
                .block()

        addressMinedBlockRepository
                .saveAll(blocksToCommit.map { block -> CqlEthereumAddressMinedBlock(block) })
                .collectList().block()
        addressMinedBlockRepository
                .deleteAll(blocksToRevert.map { block -> CqlEthereumAddressMinedBlock(block) })
                .block()

        if (::topicCurrentOffsetMonitor.isInitialized) {
            topicCurrentOffsetMonitor.set(records.last().offset())
        } else {
            topicCurrentOffsetMonitor = monitoring.gauge("dump_topic_current_offset",
                    Tags.of("topic", chain.blockPumpTopic), AtomicLong(records.last().offset()))!!
        }

    }
}
