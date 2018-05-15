package fund.cyber.pump.common.kafka

import fund.cyber.common.kafka.kafkaTopicName
import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional


@Component
class KafkaBlockBundleProducer(
    private val kafkaTemplate: KafkaTemplate<PumpEvent, Any>,
    private val chainInfo: ChainInfo
) {

    @Transactional
    fun storeBlockBundle(blockBundleEvents: List<Pair<PumpEvent, BlockBundle>>) {
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
