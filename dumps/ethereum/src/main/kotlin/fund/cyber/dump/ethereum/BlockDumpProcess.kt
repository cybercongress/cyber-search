package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.repository.EthereumAddressMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.search.model.chains.Chain
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener


class BlockDumpProcess(
        private val blockRepository: EthereumBlockRepository,
        private val addressMinedBlockRepository: EthereumAddressMinedBlockRepository,
        private val chain: Chain
) : BatchMessageListener<PumpEvent, EthereumBlock> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)


    //todo add retry
    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumBlock>>) {

        val first = records.first()
        val last = records.last()
        log.info("Dumping batch of ${first.value().number}-${last.value().number} $chain blocks")

        val blocksToSave = records.filter { record -> record.key() == PumpEvent.NEW_BLOCK }
                .map { record -> record.value() }
                .map { block -> CqlEthereumBlock(block) }

        blockRepository.saveAll(blocksToSave).collectList().block()

        val blocksByAddressToSave = records.filter { record -> record.key() == PumpEvent.NEW_BLOCK }
                .map { record -> record.value() }
                .map { block -> CqlEthereumAddressMinedBlock(block) }

        addressMinedBlockRepository.saveAll(blocksByAddressToSave).collectList().block()

    }
}