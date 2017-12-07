package fund.cyber.address


import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.*


// https://kafka.apache.org/0101/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html

abstract class AddressStateUpdater<K, V>(val topic: String) : Runnable {


    abstract val consumer: KafkaConsumer<K, V>
    abstract fun handleRecord(record: ConsumerRecord<K, V>)

    private val closed = AtomicBoolean(false)

    override fun run() {
        try {
            consumer.subscribe(listOf(topic))
            while (!closed.get()) {


                val records = consumer.poll(1000)

                for (partition in records.partitions()) {
                    val partitionRecords = records.records(partition)
                    for (record in partitionRecords) {
                        println(record.offset())
                        handleRecord(record)
                    }
                    val lastOffset = partitionRecords.get(partitionRecords.size - 1).offset()
                    consumer.commitSync(Collections.singletonMap(partition, OffsetAndMetadata(lastOffset + 1)))
                }
                consumer.commitSync()
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

    fun shutdown() {
        closed.set(true)
        consumer.wakeup()
    }
}