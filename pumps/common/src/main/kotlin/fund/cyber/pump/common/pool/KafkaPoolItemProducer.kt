package fund.cyber.pump.common.pool

import fund.cyber.common.kafka.kafkaTopicName
import fund.cyber.search.model.PoolItem
import fund.cyber.search.model.chains.ChainEntityType
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional


@Component
class KafkaPoolItemProducer(
    private val kafkaTemplate: KafkaTemplate<PumpEvent, Any>,
    private val chainInfo: ChainInfo
) {

    @Transactional
    fun storeItems(itemsEvents: List<Pair<PumpEvent, PoolItem>>) {
        itemsEvents.forEach { itemEvent ->
            val event = itemEvent.first
            val item = itemEvent.second
            kafkaTemplate.send(ChainEntityType.TX.kafkaTopicName(chainInfo), event, item)
        }
    }
}
