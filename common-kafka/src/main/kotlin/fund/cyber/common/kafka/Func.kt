package fund.cyber.common.kafka

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition

fun Consumer<Any, Any>.readTopicLastOffset(topic: String): Long {
    val partitions = partitionsFor(topic)
    if (partitions == null || partitions.size == 0) return -1L
    if (partitions.size > 1) throw RuntimeException("Topic have more than one partition")

    val partition = TopicPartition(partitions.first().topic(), partitions.first().partition())
    assign(listOf(partition))
    seekToEnd(listOf(partition))
    return position(partition) - 1
}
