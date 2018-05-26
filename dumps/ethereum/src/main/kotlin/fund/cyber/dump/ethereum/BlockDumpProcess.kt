package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.repository.EthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.dump.common.executeOperations
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import reactor.core.publisher.Mono


class BlockDumpProcess(
    private val blockRepository: EthereumBlockRepository,
    private val contractMinedBlockRepository: EthereumContractMinedBlockRepository,
    private val chain: EthereumFamilyChain
) : BatchMessageListener<PumpEvent, EthereumBlock> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumBlock>>) {

        log.info("Dumping batch of ${records.size} $chain blocks from offset ${records.first().offset()}")

        records.executeOperations { event, block ->
            return@executeOperations when (event) {
                PumpEvent.NEW_BLOCK -> block.toNewBlockPublisher()
                PumpEvent.NEW_POOL_TX -> Mono.empty()
                PumpEvent.DROPPED_BLOCK -> block.toDropBlockPublisher()
            }
        }
    }

    private fun EthereumBlock.toNewBlockPublisher(): Publisher<Any> {
        val saveBlockMono = blockRepository.save(CqlEthereumBlock(this))
        val saveContractBlockMono = contractMinedBlockRepository.save(CqlEthereumContractMinedBlock(this))

        return reactor.core.publisher.Flux.concat(saveBlockMono, saveContractBlockMono)
    }

    private fun EthereumBlock.toDropBlockPublisher(): Publisher<Any> {
        val deleteBlockMono = blockRepository.delete(CqlEthereumBlock(this))
        val deleteContractBlockMono = contractMinedBlockRepository.delete(CqlEthereumContractMinedBlock(this))

        return reactor.core.publisher.Flux.concat(deleteBlockMono, deleteContractBlockMono)
    }
}
