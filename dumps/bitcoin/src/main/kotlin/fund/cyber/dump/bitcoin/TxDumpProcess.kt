package fund.cyber.dump.bitcoin

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlockTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTxPreviewIO
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.dump.common.toFlux
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import java.util.concurrent.atomic.AtomicLong

@Suppress("MagicNumber")
class TxDumpProcess(
    private val txRepository: BitcoinTxRepository,
    private val contractTxRepository: BitcoinContractTxRepository,
    private val blockTxRepository: BitcoinBlockTxRepository,
    private val chain: BitcoinFamilyChain,
    monitoring: MeterRegistry
) : BatchMessageListener<PumpEvent, BitcoinTx> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    private val requestCountMonitor = monitoring.gauge(
        "dump_cs_requests_per_batch",
        Tags.of("topic", chain.txPumpTopic),
        AtomicLong(0)
    )!!

    private var requestsCounter = 0L

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, BitcoinTx>>) {

        log.info("Dumping batch of ${records.size} $chain transactions from offset ${records.first().offset()}")
        requestsCounter = 0

        records.toFlux { event, tx ->
            return@toFlux when (event) {
                PumpEvent.NEW_BLOCK -> tx.toNewBlockPublisher()
                PumpEvent.NEW_POOL_TX -> tx.toNewPoolItemPublisher()
                PumpEvent.DROPPED_BLOCK -> tx.toDropBlockPublisher()
            }
        }.collectList().block()

        requestCountMonitor.set(requestsCounter)
        log.info("Finish dump batch of ${records.size} $chain transactions from offset ${records.first().offset()}")
    }

    private fun BitcoinTx.toNewBlockPublisher(): Publisher<Any> {

        log.debug("NEW_BLOCK tx ${this.hash}")

        val saveTxMono = txRepository.findById(this.hash)
            .flatMap { cqlTx -> txRepository.save(CqlBitcoinTx(this.copy(firstSeenTime = cqlTx.firstSeenTime))) }
            .switchIfEmpty(Mono.defer { txRepository.save(CqlBitcoinTx(this)) })

        val saveBlockTxMono = blockTxRepository.save(CqlBitcoinBlockTxPreview(this))

        val affectedContracts = this.allContractsUsedInTransaction().toSet()

        val ins = this.ins.map { txIn -> CqlBitcoinTxPreviewIO(txIn) }
        val outs = this.outs.map { txOut -> CqlBitcoinTxPreviewIO(txOut) }

        val contractTxesToDelete = affectedContracts
            .map { it -> CqlBitcoinContractTxPreview(it, this.mempoolState(), ins, outs) }

        val contractTxesToSave = affectedContracts.map { it -> CqlBitcoinContractTxPreview(it, this, ins, outs) }

        val saveContractTxesFlux = Flux.merge(
            contractTxRepository.saveAll(contractTxesToSave),
            contractTxRepository.deleteAll(contractTxesToDelete)
        )

        requestsCounter += 3 + 2 * affectedContracts.size

        return Flux.merge(saveTxMono, saveBlockTxMono, saveContractTxesFlux)
    }

    private fun BitcoinTx.toDropBlockPublisher(): Publisher<Any> {

        log.debug("DROP_BLOCK tx ${this.hash}")

        val saveTxMono = txRepository.findById(this.hash)
            .flatMap { cqlTx ->
                txRepository.save(CqlBitcoinTx(this.mempoolState().copy(firstSeenTime = cqlTx.firstSeenTime)))
            }
            .switchIfEmpty(Mono.defer { txRepository.save(CqlBitcoinTx(this.mempoolState())) })

        val deleteBlockTxMono = blockTxRepository.delete(CqlBitcoinBlockTxPreview(this))

        val affectedContracts = this.allContractsUsedInTransaction().toSet()

        val ins = this.ins.map { txIn -> CqlBitcoinTxPreviewIO(txIn) }
        val outs = this.outs.map { txOut -> CqlBitcoinTxPreviewIO(txOut) }

        val deleteContractTxesFlux = contractTxRepository.deleteAll(
            affectedContracts.map { it -> CqlBitcoinContractTxPreview(it, this, ins, outs) }
        )

        requestsCounter += 3 + affectedContracts.size

        return Flux.merge(saveTxMono, deleteBlockTxMono, deleteContractTxesFlux)
    }

    private fun BitcoinTx.toNewPoolItemPublisher(): Publisher<Any> {

        log.debug("NEW_POOL tx ${this.hash}")

        val affectedContracts = this.allContractsUsedInTransaction().toSet()

        val ins = this.ins.map { txIn -> CqlBitcoinTxPreviewIO(txIn) }
        val outs = this.outs.map { txOut -> CqlBitcoinTxPreviewIO(txOut) }

        val contractTxesToSave = affectedContracts.map { it -> CqlBitcoinContractTxPreview(it, this, ins, outs) }

        requestsCounter += 2 + affectedContracts.size

        return txRepository.findById(this.hash)
            .map { it -> it as Any } // hack to convert Mono to Any type
            .toFlux()
            .switchIfEmpty(
                Flux.merge(
                    Mono.defer { txRepository.save(CqlBitcoinTx(this)) },
                    Flux.defer { contractTxRepository.saveAll(contractTxesToSave) }
                )
            )
    }

}
