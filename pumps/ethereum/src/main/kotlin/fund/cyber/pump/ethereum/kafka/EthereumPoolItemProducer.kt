package fund.cyber.pump.ethereum.kafka

import fund.cyber.pump.common.pool.KafkaPoolItemProducer
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional


@Component("kafkaPoolItemProducer")
class EthereumPoolItemProducer(
    private val kafkaTemplate: KafkaTemplate<PumpEvent, Any>,
    private val chain: EthereumFamilyChain
): KafkaPoolItemProducer<EthereumTx> {

    @Transactional
    override fun storeItems(itemsEvents: List<Pair<PumpEvent, EthereumTx>>) {
        itemsEvents.forEach { itemEvent ->
            val event = itemEvent.first
            val item = itemEvent.second
            kafkaTemplate.send(chain.txPumpTopic, event, item)
        }
    }
}
