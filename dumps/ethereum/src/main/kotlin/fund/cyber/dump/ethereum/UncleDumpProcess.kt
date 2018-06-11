package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedUncle
import fund.cyber.cassandra.ethereum.model.CqlEthereumUncle
import fund.cyber.cassandra.ethereum.repository.EthereumContractUncleRepository
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
import fund.cyber.dump.common.toFlux
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono


class UncleDumpProcess(
    private val uncleRepository: EthereumUncleRepository,
    private val contractUncleRepository: EthereumContractUncleRepository,
    private val chain: ChainInfo
) : BatchMessageListener<PumpEvent, EthereumUncle> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumUncle>>) {

        log.info("Dumping batch of ${records.size} ${chain.name} uncles from offset ${records.first().offset()}")

        records.toFlux { event, uncle ->
            return@toFlux when (event) {
                PumpEvent.NEW_BLOCK -> uncle.toNewBlockPublisher()
                PumpEvent.NEW_POOL_TX -> Mono.empty()
                PumpEvent.DROPPED_BLOCK -> uncle.toDropBlockPublisher()
            }
        }.collectList().block()
    }

    private fun EthereumUncle.toNewBlockPublisher(): Publisher<Any> {
        val saveBlockMono = uncleRepository.save(CqlEthereumUncle(this))
        val saveContractBlockMono = contractUncleRepository.save(CqlEthereumContractMinedUncle(this))

        return Flux.merge(saveBlockMono, saveContractBlockMono)
    }

    private fun EthereumUncle.toDropBlockPublisher(): Publisher<Any> {
        val deleteBlockMono = uncleRepository.delete(CqlEthereumUncle(this))
        val deleteContractBlockMono = contractUncleRepository.delete(CqlEthereumContractMinedUncle(this))

        return Flux.merge(deleteBlockMono, deleteContractBlockMono)
    }
}
