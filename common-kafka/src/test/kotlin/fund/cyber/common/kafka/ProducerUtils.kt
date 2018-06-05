package fund.cyber.common.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.support.serializer.JsonSerializer
import java.util.*

private val jsonSerializer = JsonSerializer<Any>()


fun <K, V> sendRecordsInTransaction(kafkaBrokers: String, topic: String, records: List<Pair<K, V>>) {

    val configuration = producerProperties(kafkaBrokers).apply {
        put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString())
    }
    val producer = KafkaProducer<Any, Any>(configuration, jsonSerializer, jsonSerializer).apply { initTransactions() }


    producer.beginTransaction()
    records.forEach { record ->
        producer.send(ProducerRecord(topic, record.first, record.second))
    }
    producer.commitTransaction()
}


fun <K, V> sendRecords(kafkaBrokers: String, topic: String, records: List<Pair<K, V>>) {

    val configuration = producerProperties(kafkaBrokers)
    val producer = KafkaProducer<Any, Any>(configuration, jsonSerializer, jsonSerializer)

    records.forEach { record ->
        producer.send(ProducerRecord(topic, record.first, record.second)).get()
    }
}


private const val BUFFER_MEMORY_CONFIG_VALUE = 33554432
private const val BATCH_SIZE_CONFIG_VALUE = 16384

fun producerProperties(kafkaBrokers: String) = Properties().apply {
    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    put(ProducerConfig.RETRIES_CONFIG, 1)
    put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE_CONFIG_VALUE)
    put(ProducerConfig.LINGER_MS_CONFIG, 1)
    put(ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFER_MEMORY_CONFIG_VALUE)
}
