package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumBlockTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.dump.common.execute
import fund.cyber.dump.common.toFluxBatch
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import reactor.core.publisher.Flux
import reactor.core.publisher.toFlux


//todo in dump add label for smart contract from calls
class TxDumpProcess(
    private val txRepository: EthereumTxRepository,
    private val blockTxRepository: EthereumBlockTxRepository,
    private val contractTxRepository: EthereumContractTxRepository,
    private val chain: EthereumFamilyChain
) : BatchMessageListener<PumpEvent, EthereumTx> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumTx>>) {

        log.info("Dumping batch of ${records.size} $chain transactions from offset ${records.first().offset()}")

        records.toFluxBatch { event, tx ->
            return@toFluxBatch when (event) {
                PumpEvent.NEW_BLOCK -> tx.toNewBlockPublisher()
                PumpEvent.NEW_POOL_TX -> tx.toNewPoolItemPublisher()
                PumpEvent.DROPPED_BLOCK -> tx.toDropBlockPublisher()
            }
        }.execute()

    }

    private fun EthereumTx.toNewBlockPublisher(): Publisher<Any> {

        val saveTxMono = txRepository.findById(this.hash)
            .flatMap { cqlTx -> txRepository.save(CqlEthereumTx(this.copy(firstSeenTime = cqlTx.firstSeenTime))) }
            .switchIfEmpty(txRepository.save(CqlEthereumTx(this)))

        val saveBlockTxMono = blockTxRepository.save(CqlEthereumBlockTxPreview(this))


        val contractTxesToDelete = this.contractsUsedInTransaction().toSet()
            .map { it -> CqlEthereumContractTxPreview(this.mempoolState(), it) }

        val contractTxesToSave = this.contractsUsedInTransaction().toSet()
            .map { it -> CqlEthereumContractTxPreview(this, it) }

        val saveContractTxesFlux = Flux.concat(
            contractTxRepository.saveAll(contractTxesToSave),
            contractTxRepository.deleteAll(contractTxesToDelete)
        )

        return Flux.concat(saveTxMono, saveBlockTxMono, saveContractTxesFlux)
    }

    private fun EthereumTx.toDropBlockPublisher(): Publisher<Any> {

        val saveTxMono = txRepository.findById(this.hash)
            .flatMap { cqlTx ->
                txRepository.save(CqlEthereumTx(this.mempoolState().copy(firstSeenTime = cqlTx.firstSeenTime)))
            }
            .switchIfEmpty(txRepository.save(CqlEthereumTx(this.mempoolState())))

        val deleteBlockTxMono = blockTxRepository.delete(CqlEthereumBlockTxPreview(this))

        val deleteContractTxesFlux = contractTxRepository.deleteAll(
            this.contractsUsedInTransaction().toSet().map { it -> CqlEthereumContractTxPreview(this, it) }
        )

        return Flux.concat(saveTxMono, deleteBlockTxMono, deleteContractTxesFlux)
    }

    private fun EthereumTx.toNewPoolItemPublisher(): Publisher<Any> {

        val contractTxesToSave = this.contractsUsedInTransaction().toSet()
            .map { it -> CqlEthereumContractTxPreview(this, it) }

        return txRepository.findById(this.hash)
            .map { it -> it as Any } // hack to convert Mono to Any type
            .toFlux()
            .switchIfEmpty(
                Flux.concat(txRepository.save(CqlEthereumTx(this)), contractTxRepository.saveAll(contractTxesToSave))
            )
    }

}
