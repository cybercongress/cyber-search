package fund.cyber.common.kafka.reader

import fund.cyber.common.kafka.JsonDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.*

const val CONSUMER_POLL_TIMEOUT = 100L
const val EMPTY_POLL_MAX_NUMBER = 5

class SinglePartitionTopicLastItemsReader<out K, out V>(
        private val kafkaBrokers: String,
        private val topic: String,
        keyClass: Class<K>,
        valueClass: Class<V>
) {

    private val consumerProperties = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    }

    private val consumer =
            KafkaConsumer<K, V>(consumerProperties, JsonDeserializer(keyClass), JsonDeserializer(valueClass))


    fun readLastRecords(numberOfRecordsToRead: Int): List<Pair<K, V>> {

        val partitions = consumer.partitionsFor(topic)
        if (partitions == null || partitions.size == 0) return emptyList()
        if (partitions.size > 1) throw RuntimeException("Topic have more than one partition")

        val partition = TopicPartition(partitions.first().topic(), partitions.first().partition())
        consumer.assign(listOf(partition))
        consumer.seekToEnd(listOf(partition))
        val lastTopicItemOffset = consumer.position(partition) - 1

        if (lastTopicItemOffset == -1L) return emptyList()


        val records: MutableList<Pair<K, V>> = mutableListOf()

        var readOffsetNumber = 0
        var emptyPollNumber = 0
        while (records.size != numberOfRecordsToRead && lastTopicItemOffset - readOffsetNumber >= 0) {

            consumer.seek(partition, lastTopicItemOffset - readOffsetNumber)
            val consumerRecords = consumer.poll(CONSUMER_POLL_TIMEOUT)

            if (consumerRecords.isEmpty && emptyPollNumber < EMPTY_POLL_MAX_NUMBER) {
                emptyPollNumber++
                continue
            }

            if (!consumerRecords.isEmpty) {
                val record = consumerRecords.first()
                records.add(record.key() to record.value())
            }

            readOffsetNumber++
            emptyPollNumber = 0
        }

        return records
    }

    fun readLastOffset(partitionIndex: Int): Long {
        val partitions = consumer.partitionsFor(topic)?.sortedBy { p -> p.partition() }
        if (partitions == null || partitions.isEmpty() || partitionIndex >= partitions.size) return 0

        val partition = TopicPartition(topic, partitionIndex)
        consumer.assign(listOf(partition))
        consumer.seekToEnd(listOf(partition))
        return consumer.position(partition) - 1
    }
}
