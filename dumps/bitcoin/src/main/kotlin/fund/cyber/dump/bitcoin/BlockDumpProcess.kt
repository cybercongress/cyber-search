package fund.cyber.dump.bitcoin

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.chains.Chain
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener


class BlockDumpProcess(
        private val blockRepository: BitcoinBlockRepository,
        private val chain: Chain
) : BatchMessageListener<PumpEvent, BitcoinBlock> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)


    override fun onMessage(records: List<ConsumerRecord<PumpEvent, BitcoinBlock>>) {

        val first = records.first()
        val last = records.last()
        log.info("Dumping batch of ${first.value().height}-${last.value().height} $chain blocks")

        val blocksToSave = records.filter { record -> record.key() == PumpEvent.NEW_BLOCK }
                .map { record -> record.value() }
                .map { block -> CqlBitcoinBlock(block) }

        blockRepository.saveAll(blocksToSave).collectList().block()
    }
}