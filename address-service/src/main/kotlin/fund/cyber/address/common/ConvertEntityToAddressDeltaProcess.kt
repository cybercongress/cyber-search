package fund.cyber.address.common

import fund.cyber.address.ServiceConfiguration
import fund.cyber.node.common.Chain
import fund.cyber.node.common.awaitAll
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.JsonSerializer
import fund.cyber.node.kafka.KafkaConsumerRunner
import fund.cyber.node.kafka.KafkaEvent
import fund.cyber.node.model.CyberSearchItem
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.util.*

typealias  ConvertItemToAddressDeltaFunction<T> = (T) -> List<AddressDelta>

private val log = LoggerFactory.getLogger(ConvertEntityToAddressDeltaProcess::class.java)!!

class ConvertEntityToAddressDeltaProcess<T : CyberSearchItem>(
        private val chain: Chain,
        private val parameters: ConvertEntityToAddressDeltaProcessParameters<T>,
        private val topic: String = parameters.inputTopic
) : KafkaConsumerRunner<KafkaEvent, T>(listOf(topic)) {

    private var lastProcessedItemBlock = -1L

    private fun addressDeltaRecord(delta: AddressDelta): ProducerRecord<Any, Any> {
        return ProducerRecord(chain.addressDeltaTopic, delta)
    }

    private val consumerGroup = "${topic}_ADDRESS_UPDATES_EMITING_PROCESS_CONSUMER"

    private val consumerProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", consumerGroup)
        put("isolation.level", "read_committed")
        put("enable.auto.commit", false)
        put("auto.offset.reset", "earliest")
    }

    private val producerProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", "${topic}_ADDRESS_UPDATES_EMITING_PROCESS_PRODUCER")
        put("transactional.id", "${topic}_ADDRESS_UPDATES_EMITING_PROCESS_TRANSACTION")
    }

    override val consumer by lazy {
        KafkaConsumer<KafkaEvent, T>(
                consumerProperties,
                JsonDeserializer(KafkaEvent::class.java), JsonDeserializer(parameters.entityType)
        )
    }

    private val jsonSerializer = JsonSerializer<Any>()
    private val producer by lazy {
        KafkaProducer<Any, Any>(producerProperties, jsonSerializer, jsonSerializer).apply { initTransactions() }
    }


    override fun processRecord(partition: TopicPartition, record: ConsumerRecord<KafkaEvent, T>) {

        val item = record.value() as T
        val deltas = parameters.convertEntityToAddressDeltaFunction(item)

        if (deltas.isEmpty()) return

        val blockNumber = deltas.first().blockNumber

        if (lastProcessedItemBlock != blockNumber) {
            lastProcessedItemBlock = blockNumber
            log.info("Calculating $chain addresses deltas for block $blockNumber")
        }

        producer.beginTransaction()
        try {
            deltas.map { delta -> producer.send(addressDeltaRecord(delta)) }.awaitAll()
            val offset = mapOf(partition to OffsetAndMetadata(record.offset() + 1))
            producer.sendOffsetsToTransaction(offset, consumerGroup)
            producer.commitTransaction()
        } catch (e: Exception) {
            log.error("Calculating $chain addresses deltas for block $blockNumber finished with error", e)
            producer.abortTransaction()
            Runtime.getRuntime().exit(-1)
        }
    }

    override fun shutdown() {
        super.shutdown()
        producer.close()
    }
}