package fund.cyber.address.bitcoin

import fund.cyber.address.ServiceConfiguration
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.node.kafka.ExactlyOnceKafkaConsumerRunner
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.KafkaEvent
import fund.cyber.node.model.BitcoinAddress
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.math.BigDecimal
import java.util.*


class BitcoinAddressUpdatesPersistenceProcess(
        topic: String,
        private val repository: BitcoinKeyspaceRepository
) : ExactlyOnceKafkaConsumerRunner<KafkaEvent, BitcoinAddressDelta>(listOf(topic)) {

    private val kafkaProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", "bitcoin-address-updates-persistence-process1")
        put("enable.auto.commit", false)
        put("isolation.level", "read_committed")
        put("auto.offset.reset", "earliest")
    }

    override val consumer by lazy {
        KafkaConsumer<KafkaEvent, BitcoinAddressDelta>(
                kafkaProperties,
                JsonDeserializer(KafkaEvent::class.java), JsonDeserializer(BitcoinAddressDelta::class.java)
        )
    }

    override fun processRecord(record: ConsumerRecord<KafkaEvent, BitcoinAddressDelta>) {

        val delta = record.value()
        val address = repository.addressStore.get(delta.address)

        if (address == null) {
            val newAddress = nonExistingAddressFromDelta(delta)
            repository.addressStore.save(newAddress)
        } else {
            val updatedAddress = updatedAddressByDelta(address, delta)
            repository.addressStore.save(updatedAddress)
        }
    }

    private fun nonExistingAddressFromDelta(delta: BitcoinAddressDelta): BitcoinAddress {

        return BitcoinAddress(
                id = delta.address, confirmed_tx_number = 1,
                confirmed_balance = delta.delta.toString(), confirmed_total_received = delta.delta
        )
    }


    private fun updatedAddressByDelta(address: BitcoinAddress, addressDelta: BitcoinAddressDelta): BitcoinAddress {

        val sign = addressDelta.delta.signum()

        val totalReceived =
                if (sign > 0) address.confirmed_total_received + addressDelta.delta else address.confirmed_total_received

        val newBalance = (BigDecimal(address.confirmed_balance) + addressDelta.delta).toString()

        return BitcoinAddress(
                id = address.id, confirmed_tx_number = address.confirmed_tx_number + 1,
                confirmed_total_received = totalReceived, confirmed_balance = newBalance
        )
    }
}