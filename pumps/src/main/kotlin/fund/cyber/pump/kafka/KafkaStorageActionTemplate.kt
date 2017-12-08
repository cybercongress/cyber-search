package fund.cyber.pump.kafka

import fund.cyber.node.common.awaitAll
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.StorageAction
import fund.cyber.pump.StorageActionSourceFactory
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory


private val log = LoggerFactory.getLogger(KafkaStorageAction::class.java)!!

class KafkaStorageAction(
        private val producer: KafkaProducer<Any, Any>,
        private val actionTemplate: KafkaStorageActionTemplate
) : StorageAction {

    //todo create common function
    override fun store() {
        producer.beginTransaction()
        try {
            actionTemplate.storeRecords.map { record -> producer.send(record) }.awaitAll()
        } catch (e: Exception) {
            log.error("Error execution kafka action", e)
            producer.abortTransaction()
            return
        }
        producer.commitTransaction()
    }

    override fun remove() {
        producer.beginTransaction()
        try {
            actionTemplate.removeRecords.map { record -> producer.send(record) }.awaitAll()
        } catch (e: Exception) {
            log.error("Error execution kafka action", e)
            producer.abortTransaction()
            return
        }
        producer.commitTransaction()
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