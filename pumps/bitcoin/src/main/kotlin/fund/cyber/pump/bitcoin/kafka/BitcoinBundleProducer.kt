package fund.cyber.pump.bitcoin.kafka

import fund.cyber.pump.bitcoin.client.BitcoinBlockBundle
import fund.cyber.pump.common.KafkaBlockBundleProducer
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class BitcoinBlockBundleProducer(
        private val kafkaTemplate: KafkaTemplate<PumpEvent, Any>,
        private val chain: BitcoinFamilyChain
) : KafkaBlockBundleProducer<BitcoinBlockBundle> {

    @Transactional
    override fun storeBlockBundle(blockBundle: BitcoinBlockBundle) {
        kafkaTemplate.send(chain.blockPumpTopic, PumpEvent.NEW_BLOCK, blockBundle.block)
        blockBundle.transactions.forEach { tx -> kafkaTemplate.send(chain.txPumpTopic, PumpEvent.NEW_BLOCK, tx) }
    }
}