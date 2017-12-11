package fund.cyber.address.common

import fund.cyber.address.ServiceConfiguration
import fund.cyber.node.common.Chain
import fund.cyber.node.kafka.ExactlyOnceKafkaConsumerRunner
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.KafkaEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import java.util.*


typealias ApplyAddressDeltaFunction = (AddressDelta) -> Unit

private val log = LoggerFactory.getLogger(ApplyingAddressDeltasProcess::class.java)!!

class ApplyingAddressDeltasProcess(
        private val chain: Chain,
        private val updateAddressStateByDeltaFunction: ApplyAddressDeltaFunction
) : ExactlyOnceKafkaConsumerRunner<KafkaEvent, AddressDelta>(listOf(chain.addressDeltaTopic)) {

    private var lastProcessedItemBlock = -1L

    private val kafkaProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", "bitcoin-address-updates-persistence-process")
        put("enable.auto.commit", false)
        put("isolation.level", "read_committed")
        put("auto.offset.reset", "earliest")
    }

    override val consumer by lazy {
        KafkaConsumer<KafkaEvent, AddressDelta>(
                kafkaProperties,
                JsonDeserializer(KafkaEvent::class.java), JsonDeserializer(AddressDelta::class.java)
        )
    }

    override fun processRecord(record: ConsumerRecord<KafkaEvent, AddressDelta>) {

        val addressDelta = record.value()

        val blockNumber = addressDelta.blockNumber

        if (lastProcessedItemBlock != blockNumber) {
            lastProcessedItemBlock = blockNumber
            log.info("Applying $chain addresses deltas for block $blockNumber")
        }

        try {
            updateAddressStateByDeltaFunction(addressDelta)
        } catch (e: Exception) {
            log.error("Calculating $chain addresses deltas for address ${addressDelta.address} finished with error", e)
            Runtime.getRuntime().exit(-1)
        }
    }
}