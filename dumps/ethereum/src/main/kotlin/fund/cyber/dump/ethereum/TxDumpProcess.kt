package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.*
import fund.cyber.cassandra.ethereum.repository.*
import fund.cyber.search.model.chains.Chain
import fund.cyber.search.model.ethereum.EthereumTransaction
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener

class TxDumpProcess(
        private val txRepository: EthereumTxRepository,
        private val blockTxRepository: EthereumBlockTxRepository,
        private val addressTxRepository: EthereumAddressTxRepository,
        private val chain: Chain
) : BatchMessageListener<PumpEvent, EthereumTransaction> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)


    //todo add retry
    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumTransaction>>) {

        log.info("Dumping batch of ${records.size} $chain txs")

        val txsToSave = records.filter { record -> record.key() == PumpEvent.NEW_BLOCK }
                .map { record -> record.value() }
                .map { tx -> CqlEthereumTransaction(tx) }

        txRepository.saveAll(txsToSave).collectList().block()

        val txsByBlockToSave = records.filter { record -> record.key() == PumpEvent.NEW_BLOCK }
                .map { record -> record.value() }
                .map { tx -> CqlEthereumBlockTxPreview(tx) }

        blockTxRepository.saveAll(txsByBlockToSave).collectList().block()

        val txsByAddressToSave = records.filter { record -> record.key() == PumpEvent.NEW_BLOCK }
                .map { record -> record.value() }
                .flatMap { tx -> tx.addressesUsedInTransaction().map { it -> CqlEthereumAddressTxPreview(tx, it) } }

        addressTxRepository.saveAll(txsByAddressToSave).collectList().block()

    }
}