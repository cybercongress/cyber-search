package fund.cyber.address.common

import fund.cyber.address.ServiceConfiguration
import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity
import fund.cyber.node.kafka.ExactlyOnceKafkaConsumerRunner
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import java.util.*


val Chain.addressCache: String get() = name + "_" + ChainEntity.ADDRESS

typealias ApplyAddressDeltaFunction<D> = (D) -> Unit

private val log = LoggerFactory.getLogger(ApplyingAddressDeltasProcess::class.java)!!


//todo apply CAS and  || execution via partitions
class ApplyingAddressDeltasProcess<D : AddressDelta>(
        private val chain: Chain,
        private val addressDeltaClassType: Class<D>,
        private val updateAddressStateByDeltaFunction: ApplyAddressDeltaFunction<D>
) : ExactlyOnceKafkaConsumerRunner<PumpEvent, D>(listOf(chain.addressDeltaTopic)) {

    private var lastProcessedItemBlock = -1L

    private val kafkaProperties = Properties().apply {
        put("bootstrap.servers", ServiceConfiguration.kafkaBrokers)
        put("group.id", "$chain-apply-address-deltas-process")
        put("enable.auto.commit", false)
        put("isolation.level", "read_committed")
        put("auto.offset.reset", "earliest")
    }

    override val consumer by lazy {
        KafkaConsumer<PumpEvent, D>(
                kafkaProperties, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(addressDeltaClassType)
        )
    }

    override fun processRecord(record: ConsumerRecord<PumpEvent, D>) {

        val addressDelta = record.value()

        val blockNumber = addressDelta.blockNumber

        if (lastProcessedItemBlock != blockNumber) {
            lastProcessedItemBlock = blockNumber
            log.info("Applying $chain addresses deltas for block $blockNumber")
        }

        try {
            updateAddressStateByDeltaFunction(addressDelta)
        } catch (e: Exception) {
            log.error("Applying $chain addresses deltas for address ${addressDelta.address} finished with error", e)
            Runtime.getRuntime().exit(-1)
        }
    }
}