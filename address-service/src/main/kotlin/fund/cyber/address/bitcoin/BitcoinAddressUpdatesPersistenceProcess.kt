package fund.cyber.address.bitcoin

import fund.cyber.address.ServiceConfiguration
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.node.kafka.ExactlyOnceKafkaConsumerRunner
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.KafkaEvent
import fund.cyber.node.model.BitcoinAddress
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*

//todo move topic from here to common place
class BitcoinAddressUpdatesPersistenceProcess(
        topic: String,
        private val repository: BitcoinKeyspaceRepository
) : ExactlyOnceKafkaConsumerRunner<KafkaEvent, BitcoinAddress>(listOf(topic)) {


    private val kafkaProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", "bitcoin-address-updates-persistence-process")
        put("enable.auto.commit", false)
        put("auto.offset.reset", "earliest")
    }

    override val consumer by lazy {
        KafkaConsumer<KafkaEvent, BitcoinAddress>(
                kafkaProperties,
                JsonDeserializer(KafkaEvent::class.java), JsonDeserializer(BitcoinAddress::class.java)
        )
    }

    override fun processRecord(record: ConsumerRecord<KafkaEvent, BitcoinAddress>) {
        repository.addressStore.save(record.value())
    }
}