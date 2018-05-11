package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.repository.EthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.dump.common.filterNotContainsAllEventsOf
import fund.cyber.dump.common.toRecordEventsMap
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
    private val contractMinedBlockRepository: EthereumContractMinedBlockRepository,
    private val chain: EthereumFamilyChain,
    monitoring: MeterRegistry
) : BatchMessageListener<PumpEvent, EthereumBlock> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    private var topicCurrentOffsetMonitor: AtomicLong = monitoring.gauge("dump_topic_current_offset",
        Tags.of("topic", chain.blockPumpTopic), AtomicLong(0))!!


    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumBlock>>) {

        log.info("Dumping batch of ${records.size} $chain blocks from offset ${records.first().offset()}")

        val recordsToProcess = records.toRecordEventsMap()
            .filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK))

        val blocksToCommit = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.NEW_BLOCK) }.keys
        val blocksToRevert = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.DROPPED_BLOCK) }.keys


        blockRepository
            .deleteAll(blocksToRevert.map { block -> CqlEthereumBlock(block) })
            .block()
        blockRepository
            .saveAll(blocksToCommit.map { block -> CqlEthereumBlock(block) })
            .collectList().block()

        contractMinedBlockRepository
            .deleteAll(blocksToRevert.map { block -> CqlEthereumContractMinedBlock(block) })
            .block()
        contractMinedBlockRepository
            .saveAll(blocksToCommit.map { block -> CqlEthereumContractMinedBlock(block) })
            .collectList().block()

        topicCurrentOffsetMonitor.set(records.last().offset())

    }
}
