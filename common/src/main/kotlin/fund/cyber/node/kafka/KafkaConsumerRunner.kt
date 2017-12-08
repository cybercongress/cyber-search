package fund.cyber.node.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import java.util.concurrent.atomic.AtomicBoolean


abstract class KafkaConsumerRunner<K, V>(private val topics: List<String>) : Runnable {

    abstract protected val consumer: KafkaConsumer<K, V>
    abstract fun processRecord(partition: TopicPartition, record: ConsumerRecord<K, V>)

    private val closed = AtomicBoolean(false)

    override fun run() {
        try {
            consumer.subscribe(topics)
            while (!closed.get()) {
                readAndProcessRecords()
            }
        } catch (e: Exception) {
            e.printStackTrace()
        } catch (e: WakeupException) {
            // Ignore exception if closing
            if (!closed.get()) throw e
        } finally {
            consumer.close()
        }
    }

    private fun readAndProcessRecords() {

        val records = consumer.poll(0)

        for (partition in records.partitions()) {
            val partitionRecords = records.records(partition)
            for (record in partitionRecords) {
                processRecord(partition, record)
            }
        }
    }

    open fun shutdown() {
        closed.set(true)
        consumer.wakeup()
    }
}