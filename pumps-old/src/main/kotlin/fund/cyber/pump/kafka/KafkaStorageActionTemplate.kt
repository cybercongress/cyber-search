package fund.cyber.pump.kafka

import fund.cyber.node.common.awaitAll
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.StorageAction
import fund.cyber.pump.StorageActionSourceFactory
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord


class KafkaStorageAction(
        private val producer: KafkaProducer<Any, Any>,
        private val actionTemplate: KafkaStorageActionTemplate
) : StorageAction {

    override fun store() {
        producer.beginTransaction()
        try {
            actionTemplate.storeRecords.map { record -> producer.send(record) }.awaitAll()
            producer.commitTransaction()
        } catch (e: Exception) {
            producer.abortTransaction()
            throw RuntimeException(e)
        }
    }

    override fun remove() {
        producer.beginTransaction()
        try {
            actionTemplate.removeRecords.map { record -> producer.send(record) }.awaitAll()
            producer.commitTransaction()
        } catch (e: Exception) {
            producer.abortTransaction()
            throw RuntimeException(e)
        }
    }
}

interface KafkaStorageActionTemplateFactory<in T : BlockBundle> : StorageActionSourceFactory {
    fun constructActionTemplate(bundle: T): KafkaStorageActionTemplate
}


@Suppress("UNCHECKED_CAST")
class KafkaStorageActionTemplate(
        storeRecords: List<ProducerRecord<*, *>>,
        removeRecords: List<ProducerRecord<*, *>>
) {
    val storeRecords: List<ProducerRecord<Any, Any>> = storeRecords as List<ProducerRecord<Any, Any>>
    val removeRecords: List<ProducerRecord<Any, Any>> = removeRecords as List<ProducerRecord<Any, Any>>
}