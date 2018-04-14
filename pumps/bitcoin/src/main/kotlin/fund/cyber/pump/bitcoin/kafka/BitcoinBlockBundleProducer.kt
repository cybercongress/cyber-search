package fund.cyber.pump.bitcoin.kafka

import fund.cyber.pump.bitcoin.client.BitcoinBlockBundle
import fund.cyber.pump.common.kafka.KafkaBlockBundleProducer
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component("kafkaBlockBundleProducer")
class BitcoinBlockBundleProducer(
        private val kafkaTemplate: KafkaTemplate<PumpEvent, Any>,
        private val chain: BitcoinFamilyChain
) : KafkaBlockBundleProducer<BitcoinBlockBundle> {

    @Transactional
    override fun storeBlockBundle(blockBundleEvents: List<Pair<PumpEvent, BitcoinBlockBundle>>) {
        blockBundleEvents.forEach { event ->
            val eventKey = event.first
            val blockBundle = event.second
            kafkaTemplate.send(chain.blockPumpTopic, eventKey, blockBundle.block)
            blockBundle.transactions.forEach { tx -> kafkaTemplate.send(chain.txPumpTopic, eventKey, tx) }
        }
    }
}
