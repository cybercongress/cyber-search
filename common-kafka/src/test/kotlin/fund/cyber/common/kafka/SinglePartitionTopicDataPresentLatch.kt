package fund.cyber.common.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.*
import java.util.concurrent.CountDownLatch


class SinglePartitionTopicDataPresentLatch<out K, out V>(
        private val kafkaBrokers: String,
        topic: String,
        keyClass: Class<K>,
        valueClass: Class<V>
) {

    val countDownLatch = CountDownLatch(1)

    private val consumerProperties = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    }

    private val consumer =
            KafkaConsumer<K, V>(consumerProperties, JsonDeserializer(keyClass), JsonDeserializer(valueClass))

    init {
        val partitions = consumer.partitionsFor(topic)
        if (partitions == null || partitions.size == 0) throw RuntimeException("There are no partitions for topic")
        if (partitions.size > 1) throw RuntimeException("Topic have more than one partition")

        val partition = TopicPartition(partitions.first().topic(), partitions.first().partition())
        consumer.assign(listOf(partition))

        while (consumer.position(partition) <= 0) {
            consumer.seekToEnd(listOf(partition))
        }
        countDownLatch.countDown()
    }
}