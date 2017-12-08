package fund.cyber.node.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition


// https://kafka.apache.org/0101/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html

abstract class ExactlyOnceKafkaConsumerRunner<K, V>(topics: List<String>) : KafkaConsumerRunner<K, V>(topics) {

    final override fun processRecord(partition: TopicPartition, record: ConsumerRecord<K, V>) {
        processRecord(record)
        val offset = mapOf(partition to OffsetAndMetadata(record.offset() + 1))
        consumer.commitSync(offset)
    }

    abstract fun processRecord(record: ConsumerRecord<K, V>)
}