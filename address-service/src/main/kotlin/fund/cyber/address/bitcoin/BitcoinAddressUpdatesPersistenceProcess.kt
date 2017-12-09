package fund.cyber.address.bitcoin

import fund.cyber.address.ServiceConfiguration
import fund.cyber.address.common.AddressDelta
import fund.cyber.address.common.addressDeltaTopic
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.node.common.Chain
import fund.cyber.node.kafka.ExactlyOnceKafkaConsumerRunner
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.KafkaEvent
import fund.cyber.node.model.BitcoinAddress
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.math.BigDecimal
import java.util.*


class BitcoinAddressUpdatesPersistenceProcess(
        private val chain: Chain,
        private val repository: BitcoinKeyspaceRepository
) : ExactlyOnceKafkaConsumerRunner<KafkaEvent, AddressDelta>(listOf(chain.addressDeltaTopic)) {

    private val kafkaProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", "bitcoin-address-updates-persistence-process")
        put("enable.auto.commit", false)
        put("isolation.level", "read_committed")
        put("auto.offset.reset", "earliest")
    }

    override val consumer by lazy {
        KafkaConsumer<KafkaEvent, AddressDelta>(
                kafkaProperties,
                JsonDeserializer(KafkaEvent::class.java), JsonDeserializer(AddressDelta::class.java)
        )
    }

    override fun processRecord(record: ConsumerRecord<KafkaEvent, AddressDelta>) {

        val addressDelta = record.value()
        val address = repository.addressStore.get(addressDelta.address)

        if (address == null) {
            val newAddress = nonExistingAddressFromDelta(addressDelta)
            repository.addressStore.save(newAddress)
        } else {
            val updatedAddress = updatedAddressByDelta(address, addressDelta)
            repository.addressStore.save(updatedAddress)
        }
    }

    private fun nonExistingAddressFromDelta(delta: AddressDelta): BitcoinAddress {

        return BitcoinAddress(
                id = delta.address, confirmed_tx_number = 1,
                confirmed_balance = delta.delta.toString(), confirmed_total_received = delta.delta
        )
    }


    private fun updatedAddressByDelta(address: BitcoinAddress, addressDelta: AddressDelta): BitcoinAddress {

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