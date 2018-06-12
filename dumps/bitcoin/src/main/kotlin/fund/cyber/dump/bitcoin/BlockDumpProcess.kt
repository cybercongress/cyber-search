package fund.cyber.dump.bitcoin

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractMinedBlock
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractMinedBlockRepository
import fund.cyber.dump.common.toFlux
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.chains.ChainInfo
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
    private val chainInfo: ChainInfo
) : BatchMessageListener<PumpEvent, BitcoinBlock> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, BitcoinBlock>>) {

        log.info("Dumping batch of ${records.size} ${chainInfo.name} blocks from offset ${records.first().offset()}")

        records.toFlux { event, block ->
            return@toFlux when (event) {
                PumpEvent.NEW_BLOCK -> block.toNewBlockPublisher()
                PumpEvent.NEW_POOL_TX -> Mono.empty()
                PumpEvent.DROPPED_BLOCK -> block.toDropBlockPublisher()
            }
        }.collectList().block()

        log.info("Finish dump batch of ${records.size} ${chainInfo.name} blocks from" +
            " offset ${records.first().offset()}")
    }

    private fun BitcoinBlock.toNewBlockPublisher(): Publisher<Any> {
        val saveBlockMono = blockRepository.save(CqlBitcoinBlock(this))
        val saveContractBlockMono = contractMinedBlockRepository.save(CqlBitcoinContractMinedBlock(this))

        return Flux.merge(saveBlockMono, saveContractBlockMono)
    }

    private fun BitcoinBlock.toDropBlockPublisher(): Publisher<Any> {
        val deleteBlockMono = blockRepository.delete(CqlBitcoinBlock(this))
        val deleteContractBlockMono = contractMinedBlockRepository.delete(CqlBitcoinContractMinedBlock(this))

        return Flux.merge(deleteBlockMono, deleteContractBlockMono)
    }
}
