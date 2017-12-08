package fund.cyber.address.bitcoin

import fund.cyber.address.ServiceConfiguration
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.node.common.awaitAll
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.JsonSerializer
import fund.cyber.node.kafka.KafkaConsumerRunner
import fund.cyber.node.kafka.KafkaEvent
import fund.cyber.node.model.BitcoinAddress
import fund.cyber.node.model.BitcoinTransaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import java.util.*


class BitcoinAddressStateUpdatesEmitingProcess(
        inputTopic: String,
        private val outputTopic: String,
        private val repository: BitcoinKeyspaceRepository
) : KafkaConsumerRunner<KafkaEvent, BitcoinTransaction>(listOf(inputTopic)) {


    private fun addressRecord(address: BitcoinAddress): ProducerRecord<Any, Any> {
        return ProducerRecord(outputTopic, address)
    }

    private val converter = BitcoinAddressConverter()

    private val consumerGroup = "bitcoin-address-updates-emiting-process-consumer"

    private val consumerProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", consumerGroup)
        put("isolation.level", "read_committed")
        put("enable.auto.commit", false)
        put("auto.offset.reset", "earliest")
    }

    private val producerProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", "bitcoin-address-updates-emiting-process-producer")
        put("transactional.id", "bitcoin-address-updates-emiting-process-producer")
    }

    override val consumer by lazy {
        KafkaConsumer<KafkaEvent, BitcoinTransaction>(
                consumerProperties,
                JsonDeserializer(KafkaEvent::class.java), JsonDeserializer(BitcoinTransaction::class.java)
        )
    }

    private val jsonSerializer = JsonSerializer<Any>()
    private val producer by lazy {
        KafkaProducer<Any, Any>(producerProperties, jsonSerializer, jsonSerializer).apply { initTransactions() }
    }


    override fun processRecord(partition: TopicPartition, record: ConsumerRecord<KafkaEvent, BitcoinTransaction>) {

        println(record.offset())
        val newTx = record.value()
        val affectedAddresses = newTx.allAddressesUsedInTransaction()

        val currentAddressStats = affectedAddresses
                .map { address -> repository.addressStore.getAsync(address) }.awaitAll().filterNotNull()

        val updatedAddressStates = converter.updateAddressesSummary(listOf(newTx), currentAddressStats)

        producer.beginTransaction()
        try {
            updatedAddressStates.map { address -> producer.send(addressRecord(address)) }.awaitAll()
            val offset = mapOf(partition to OffsetAndMetadata(record.offset() + 1))
            producer.sendOffsetsToTransaction(offset, consumerGroup)
            producer.commitTransaction()
        } catch (e: Exception) {
            producer.abortTransaction()
        }
    }

    override fun shutdown() {
        super.shutdown()
        producer.close()
    }
}