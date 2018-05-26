package fund.cyber.dump.bitcoin

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlockTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractTxPreview
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.dump.common.executeOperations
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import reactor.core.publisher.Flux
import reactor.core.publisher.toFlux


class TxDumpProcess(
    private val txRepository: BitcoinTxRepository,
    private val contractTxRepository: BitcoinContractTxRepository,
    private val blockTxRepository: BitcoinBlockTxRepository,
    private val chain: BitcoinFamilyChain
) : BatchMessageListener<PumpEvent, BitcoinTx> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, BitcoinTx>>) {

        log.info("Dumping batch of ${records.size} $chain transactions from offset ${records.first().offset()}")

        records.executeOperations { event, tx ->
            return@executeOperations when (event) {
                PumpEvent.NEW_BLOCK -> tx.toNewBlockPublisher()
                PumpEvent.NEW_POOL_TX -> tx.toNewPoolItemPublisher()
                PumpEvent.DROPPED_BLOCK -> tx.toDropBlockPublisher()
            }
        }

    }

    private fun BitcoinTx.toNewBlockPublisher(): Publisher<Any> {

        val saveTxMono = txRepository.findById(this.hash)
            .flatMap { cqlTx -> txRepository.save(CqlBitcoinTx(this.copy(firstSeenTime = cqlTx.firstSeenTime))) }
            .switchIfEmpty(txRepository.save(CqlBitcoinTx(this)))

        val saveBlockTxMono = blockTxRepository.save(CqlBitcoinBlockTxPreview(this))


        val entitiesToDelete = this.allContractsUsedInTransaction().toSet()
            .map { it -> CqlBitcoinContractTxPreview(it, this.mempoolState()) }

        val entitiesToSave = this.allContractsUsedInTransaction().toSet()
            .map { it -> CqlBitcoinContractTxPreview(it, this) }

        val saveContractTxesFlux = Flux.concat(
            contractTxRepository.saveAll(entitiesToSave),
            contractTxRepository.deleteAll(entitiesToDelete)
        )

        return Flux.concat(saveTxMono, saveBlockTxMono, saveContractTxesFlux)
    }

    private fun BitcoinTx.toDropBlockPublisher(): Publisher<Any> {

        val saveTxMono = txRepository.findById(this.hash)
            .flatMap { cqlTx ->
                txRepository.save(CqlBitcoinTx(this.mempoolState().copy(firstSeenTime = cqlTx.firstSeenTime)))
            }
            .switchIfEmpty(txRepository.save(CqlBitcoinTx(this.mempoolState())))

        val deleteBlockTxMono = blockTxRepository.delete(CqlBitcoinBlockTxPreview(this))

        val deleteContractTxesFlux = contractTxRepository.deleteAll(
            this.allContractsUsedInTransaction().toSet().map { it -> CqlBitcoinContractTxPreview(it, this) }
        )

        return Flux.concat(saveTxMono, deleteBlockTxMono, deleteContractTxesFlux)
    }

    private fun BitcoinTx.toNewPoolItemPublisher(): Publisher<Any> {

        val contractTxesToSave = this.allContractsUsedInTransaction().toSet()
            .map { it -> CqlBitcoinContractTxPreview(it, this) }

        return txRepository.findById(this.hash)
            .map { it -> it as Any } // hack to convert Mono to Any type
            .toFlux()
            .switchIfEmpty(
                Flux.concat(txRepository.save(CqlBitcoinTx(this)), contractTxRepository.saveAll(contractTxesToSave))
            )
    }

}
