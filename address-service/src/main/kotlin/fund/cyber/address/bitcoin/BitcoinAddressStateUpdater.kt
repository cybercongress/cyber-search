package fund.cyber.address.bitcoin

import fund.cyber.address.ServiceConfiguration.kafkaBrokers
import fund.cyber.cassandra.CassandraService
import fund.cyber.node.common.awaitAll
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.KafkaEvent
import fund.cyber.node.model.BitcoinTransaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*


/*
//todo move topic from here to common place
class BitcoinAddressStateUpdater(
        private val cassandraService: CassandraService = ServiceContext.cassandraService
) : AddressStateUpdater<KafkaEvent, BitcoinTransaction>("bitcoin_tx") {

    private val converter = BitcoinAddressConverter()

    private val kafkaProperties = Properties().apply {
        put("bootstrap.servers", kafkaBrokers)
        put("group.id", "bitcoin-address-service")
        put("enable.auto.commit", false)
        put("auto.offset.reset", "earliest")
    }

    override val consumer by lazy {
        KafkaConsumer<KafkaEvent, BitcoinTransaction>(
                kafkaProperties,
                JsonDeserializer(KafkaEvent::class.java), JsonDeserializer(BitcoinTransaction::class.java)
        )
    }

    override fun handleRecord(record: ConsumerRecord<KafkaEvent, BitcoinTransaction>) {

        val newTx = record.value()
        val affectedAddresses = newTx.allAddressesUsedInTransaction()

        val currentAddressStats = affectedAddresses
                .map { address -> cassandraService.bitcoinRepository.addressStore.getAsync(address) }
                .awaitAll().filterNotNull()

        val updatedAddressStats = converter.updateAddressesSummary(listOf(newTx), currentAddressStats)
        val newAddressTxes = converter.transactionsPreviewsForAddresses(listOf(newTx))

        updatedAddressStats.map { address -> cassandraService.bitcoinRepository.addressStore.saveAsync(address) }
                .awaitAll()

        newAddressTxes.map { addressTx -> cassandraService.bitcoinRepository.addressTxtore.saveAsync(addressTx) }
                .awaitAll()
    }
}*/
