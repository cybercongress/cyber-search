package fund.cyber.dump.bitcoin

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractMinedBlock
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.dump.common.execute
import fund.cyber.dump.common.toFluxBatch
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono


class BlockDumpProcess(
    private val blockRepository: BitcoinBlockRepository,
    private val contractMinedBlockRepository: BitcoinContractMinedBlockRepository,
    private val chain: BitcoinFamilyChain
) : BatchMessageListener<PumpEvent, BitcoinBlock> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, BitcoinBlock>>) {

        log.info("Dumping batch of ${records.size} $chain blocks from offset ${records.first().offset()}")

        records.toFluxBatch { event, block ->
            return@toFluxBatch when (event) {
                PumpEvent.NEW_BLOCK -> block.toNewBlockPublisher()
                PumpEvent.NEW_POOL_TX -> Mono.empty()
                PumpEvent.DROPPED_BLOCK -> block.toDropBlockPublisher()
            }
        }.execute()
    }

    private fun BitcoinBlock.toNewBlockPublisher(): Publisher<Any> {
        val saveBlockMono = blockRepository.save(CqlBitcoinBlock(this))
        val saveContractBlockMono = contractMinedBlockRepository.save(CqlBitcoinContractMinedBlock(this))

        return Flux.concat(saveBlockMono, saveContractBlockMono)
    }

    private fun BitcoinBlock.toDropBlockPublisher(): Publisher<Any> {
        val deleteBlockMono = blockRepository.delete(CqlBitcoinBlock(this))
        val deleteContractBlockMono = contractMinedBlockRepository.delete(CqlBitcoinContractMinedBlock(this))

        return Flux.concat(deleteBlockMono, deleteContractBlockMono)
    }
}
