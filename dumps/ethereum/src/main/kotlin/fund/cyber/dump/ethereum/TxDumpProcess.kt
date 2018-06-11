package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumBlockTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.dump.common.toFlux
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux


//todo in dump add label for smart contract from calls
class TxDumpProcess(
    private val txRepository: EthereumTxRepository,
    private val blockTxRepository: EthereumBlockTxRepository,
    private val contractTxRepository: EthereumContractTxRepository,
    private val chain: ChainInfo,
    private val realtimeIndexationThreshold: Long
) : BatchMessageListener<PumpEvent, EthereumTx> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumTx>>) {

        log.info("Dumping batch of ${records.size} ${chain.name} transactions from offset ${records.first().offset()}")

        records.toFlux { event, tx ->
            return@toFlux when (event) {
                PumpEvent.NEW_BLOCK -> tx.toNewBlockPublisher()
                PumpEvent.NEW_POOL_TX -> tx.toNewPoolItemPublisher()
                PumpEvent.DROPPED_BLOCK -> tx.toDropBlockPublisher()
            }
        }.collectList().block()

    }

    private fun EthereumTx.toNewBlockPublisher(): Publisher<Any> {

        val saveTxMono = txRepository.findById(this.hash)
            .flatMap { cqlTx -> txRepository.save(CqlEthereumTx(this.copy(firstSeenTime = cqlTx.firstSeenTime))) }
            .switchIfEmpty(Mono.defer { txRepository.save(CqlEthereumTx(this)) })

        val saveBlockTxMono = blockTxRepository.save(CqlEthereumBlockTxPreview(this))


        val contractTxesToDelete = this.contractsUsedInTransaction().toSet()
            .map { it -> CqlEthereumContractTxPreview(this.mempoolState(), it) }

        val contractTxesToSave = this.contractsUsedInTransaction().toSet()
            .map { it -> CqlEthereumContractTxPreview(this, it) }

        val saveContractTxesFlux = contractTxRepository.saveAll(contractTxesToSave)

        val deleteContractTxesFlux =
            if (this.blockNumber < realtimeIndexationThreshold) {
                Flux.empty<CqlEthereumContractTxPreview>()
            } else {
                contractTxRepository.deleteAll(contractTxesToDelete)
            }

        return Flux.merge(saveTxMono, saveBlockTxMono, saveContractTxesFlux, deleteContractTxesFlux)
    }

    private fun EthereumTx.toDropBlockPublisher(): Publisher<Any> {

        val saveTxMono = txRepository.findById(this.hash)
            .flatMap { cqlTx ->
                txRepository.save(CqlEthereumTx(this.mempoolState().copy(firstSeenTime = cqlTx.firstSeenTime)))
            }
            .switchIfEmpty(Mono.defer { txRepository.save(CqlEthereumTx(this.mempoolState())) })

        val deleteBlockTxMono = blockTxRepository.delete(CqlEthereumBlockTxPreview(this))

        val deleteContractTxesFlux = contractTxRepository.deleteAll(
            this.contractsUsedInTransaction().toSet().map { it -> CqlEthereumContractTxPreview(this, it) }
        )

        return Flux.merge(saveTxMono, deleteBlockTxMono, deleteContractTxesFlux)
    }

    private fun EthereumTx.toNewPoolItemPublisher(): Publisher<Any> {

        val contractTxesToSave = this.contractsUsedInTransaction().toSet()
            .map { it -> CqlEthereumContractTxPreview(this, it) }

        return txRepository.findById(this.hash)
            .map { it -> it as Any } // hack to convert Mono<T> to Mono<Any> type
            .toFlux()
            .switchIfEmpty(
                Flux.merge(
                    Mono.defer { txRepository.save(CqlEthereumTx(this)) },
                    Flux.defer { contractTxRepository.saveAll(contractTxesToSave) }
                )
            )
    }

}
