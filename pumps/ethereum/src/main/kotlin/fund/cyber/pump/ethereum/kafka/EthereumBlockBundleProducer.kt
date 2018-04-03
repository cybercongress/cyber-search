package fund.cyber.pump.ethereum.kafka

import fund.cyber.pump.common.kafka.KafkaBlockBundleProducer
import fund.cyber.pump.ethereum.client.EthereumBlockBundle
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import fund.cyber.search.model.events.unclePumpTopic
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional


@Component("kafkaBlockBundleProducer")
class EthereumBlockBundleProducer(
        private val kafkaTemplate: KafkaTemplate<PumpEvent, Any>,
        private val chain: EthereumFamilyChain
) : KafkaBlockBundleProducer<EthereumBlockBundle> {

    @Transactional
    override fun storeBlockBundle(blockBundleEvents: List<Pair<PumpEvent, EthereumBlockBundle>>) {
        blockBundleEvents.forEach { event ->
            val eventKey = event.first
            val blockBundle = event.second
            kafkaTemplate.send(chain.blockPumpTopic, eventKey, blockBundle.block)
            blockBundle.txes.forEach { tx -> kafkaTemplate.send(chain.txPumpTopic, eventKey, tx) }
            blockBundle.uncles.forEach { uncle -> kafkaTemplate.send(chain.unclePumpTopic, eventKey, uncle) }
        }
    }
}
