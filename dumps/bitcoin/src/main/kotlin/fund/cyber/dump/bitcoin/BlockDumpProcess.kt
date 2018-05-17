package fund.cyber.dump.bitcoin

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractMinedBlock
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.dump.common.filterNotContainsAllEventsOf
import fund.cyber.dump.common.toRecordEventsMap
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener


class BlockDumpProcess(
    private val blockRepository: BitcoinBlockRepository,
    private val contractMinedBlockRepository: BitcoinContractMinedBlockRepository,
    private val chain: BitcoinFamilyChain
) : BatchMessageListener<PumpEvent, BitcoinBlock> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, BitcoinBlock>>) {

        val first = records.first()
        val last = records.last()
        log.info("Dumping batch of ${first.value().height}-${last.value().height} $chain blocks")

        val recordsToProcess = records.toRecordEventsMap()
            .filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK))

        val blocksToCommit = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.NEW_BLOCK) }.keys
        val blocksToRevert = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.DROPPED_BLOCK) }.keys

        blockRepository
            .deleteAll(blocksToRevert.map { block -> CqlBitcoinBlock(block) })
            .block()
        blockRepository
            .saveAll(blocksToCommit.map { block -> CqlBitcoinBlock(block) })
            .collectList().block()

        contractMinedBlockRepository
            .deleteAll(blocksToRevert.map { block -> CqlBitcoinContractMinedBlock(block) })
            .block()
        contractMinedBlockRepository
            .saveAll(blocksToCommit.map { block -> CqlBitcoinContractMinedBlock(block) })
            .collectList().block()
    }
}
