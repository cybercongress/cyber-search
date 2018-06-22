package fund.cyber.pump.common.kafka

import fund.cyber.common.kafka.kafkaTopicName
import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component


@Component
class KafkaBlockBundleProducer(
    private val kafkaTemplate: KafkaTemplate<PumpEvent, Any>,
    private val chainInfo: ChainInfo
) {

    fun storeBlockBundle(blockBundleEvents: List<Pair<PumpEvent, BlockBundle>>) {

        kafkaTemplate.executeInTransaction {

            blockBundleEvents.forEach { event ->
                val eventKey = event.first
                val blockBundle = event.second

                chainInfo.entityTypes.forEach { type ->
                    blockBundle.entitiesByType(type).forEach { entity ->
                        kafkaTemplate.send(type.kafkaTopicName(chainInfo), eventKey, entity)
                    }
                }
            }
        }
    }
}
