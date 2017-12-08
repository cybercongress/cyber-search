package fund.cyber.address.bitcoin

import fund.cyber.address.ServiceConfiguration
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.node.common.awaitAll
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.JsonSerializer
import fund.cyber.node.kafka.KafkaConsumerRunner
import fund.cyber.node.kafka.KafkaEvent
import fund.cyber.node.model.BitcoinTransaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import java.math.BigDecimal
import java.util.*


data class BitcoinAddressDelta(
        val address: String,
        val delta: BigDecimal
)


class BitcoinAddressStateUpdatesEmitingProcess(
        inputTopic: String,
        private val outputTopic: String,
        private val repository: BitcoinKeyspaceRepository
) : KafkaConsumerRunner<KafkaEvent, BitcoinTransaction>(listOf(inputTopic)) {


    private fun addressDeltaRecord(delta: BitcoinAddressDelta): ProducerRecord<Any, Any> {
        return ProducerRecord(outputTopic, delta)
    }


    private val consumerGroup = "bitcoin-address-updates-emiting-process-consumer1"

    private val consumerProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", consumerGroup)
        put("isolation.level", "read_committed")
        put("enable.auto.commit", false)
        put("auto.offset.reset", "earliest")
    }

    private val producerProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", "bitcoin-address-updates-emiting-process-producer1")
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

        val addressesDeltasByIns = newTx.ins.flatMap { input ->
            input.addresses.map { address -> BitcoinAddressDelta(address, BigDecimal(input.amount).negate()) }
        }

        val addressesDeltasByOuts = newTx.outs.flatMap { output ->
            output.addresses.map { address -> BitcoinAddressDelta(address, BigDecimal(output.amount)) }
        }
        val deltas = addressesDeltasByIns + addressesDeltasByOuts

        producer.beginTransaction()
        try {
            deltas.map { delta -> producer.send(addressDeltaRecord(delta)) }.awaitAll()
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