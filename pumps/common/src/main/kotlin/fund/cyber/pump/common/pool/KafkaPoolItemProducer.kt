package fund.cyber.pump.common.pool

import fund.cyber.common.kafka.kafkaTopicName
import fund.cyber.search.model.PoolItem
import fund.cyber.search.model.chains.ChainEntityType
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

private val log = LoggerFactory.getLogger(KafkaPoolItemProducer::class.java)!!

@Component
class KafkaPoolItemProducer(
    private val kafkaTemplatePool: KafkaTemplate<PumpEvent, Any>,
    private val chainInfo: ChainInfo
) {

    fun storeItem(itemEvent: Pair<PumpEvent, PoolItem>) {
        val event = itemEvent.first
        val item = itemEvent.second
        kafkaTemplatePool.send(ChainEntityType.TX.kafkaTopicName(chainInfo), event, item)
            .addCallback({ _ -> }) { error ->
                log.error("Error during sending mempool item to kafka", error)
            }
    }
}
