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
    override fun storeBlockBundle(blockBundles: List<EthereumBlockBundle>) {
        blockBundles.forEach { blockBundle ->
            kafkaTemplate.send(chain.blockPumpTopic, PumpEvent.NEW_BLOCK, blockBundle.block)
            blockBundle.transactions.forEach { tx -> kafkaTemplate.send(chain.txPumpTopic, PumpEvent.NEW_BLOCK, tx) }
            blockBundle.uncles.forEach { uncle -> kafkaTemplate.send(chain.unclePumpTopic, PumpEvent.NEW_BLOCK, uncle) }
        }
    }
}