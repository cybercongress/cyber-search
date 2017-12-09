package fund.cyber.address.bitcoin

import fund.cyber.address.ServiceConfiguration
import fund.cyber.address.common.AddressDelta
import fund.cyber.address.common.addressDeltaTopic
import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity.TRANSACTION
import fund.cyber.node.common.awaitAll
import fund.cyber.node.kafka.*
import fund.cyber.node.model.BitcoinTransaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.util.*


private val log = LoggerFactory.getLogger(BitcoinAddressStateUpdatesEmitingProcess::class.java)!!

class BitcoinAddressStateUpdatesEmitingProcess(
        private val chain: Chain
) : KafkaConsumerRunner<KafkaEvent, BitcoinTransaction>(listOf(chain.entityTopic(TRANSACTION))) {

    private var lastProcessedItemBlock = -1L


    private fun addressDeltaRecord(delta: AddressDelta): ProducerRecord<Any, Any> {
        return ProducerRecord(chain.addressDeltaTopic, delta)
    }

    private val consumerGroup = "${chain}_ADDRESS_UPDATES_EMITING_PROCESS_CONSUMER"

    private val consumerProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", consumerGroup)
        put("isolation.level", "read_committed")
        put("enable.auto.commit", false)
        put("auto.offset.reset", "earliest")
    }

    private val producerProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", "${chain}_ADDRESS_UPDATES_EMITING_PROCESS_PRODUCER")
        put("transactional.id", "${chain}_ADDRESS_UPDATES_EMITING_PROCESS_TRANSACTION")
    }

    override val consumer by lazy {
        KafkaConsumer<KafkaEvent, BitcoinTransaction>(
                consumerProperties,
                JsonDeserializer(KafkaEvent::class.java), JsonDeserializer(BitcoinTransaction::class.java)
        )
    }

    private val jsonSerializer = JsonSerializer<Any>()
    private val producer by lazy {
        KafkaProducer<Any, Any>(producerProperties, jsonSerializer, jsonSerializer).apply { initTransactions() }
    }


    override fun processRecord(partition: TopicPartition, record: ConsumerRecord<KafkaEvent, BitcoinTransaction>) {

        val newTx = record.value()
        val blockNumber = newTx.block_number
        log.debug("Calculating $chain addresses deltas for block $blockNumber and tx ${newTx.hash}")

        if (lastProcessedItemBlock != blockNumber) {
            lastProcessedItemBlock = blockNumber
            log.info("Calculating $chain addresses deltas for block $blockNumber")
        }

        val addressesDeltasByIns = newTx.ins.flatMap { input ->
            input.addresses.map { address -> AddressDelta(address, BigDecimal(input.amount).negate(), blockNumber) }
        }

        val addressesDeltasByOuts = newTx.outs.flatMap { output ->
            output.addresses.map { address -> AddressDelta(address, BigDecimal(output.amount), blockNumber) }
        }
        val deltas = addressesDeltasByIns + addressesDeltasByOuts

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