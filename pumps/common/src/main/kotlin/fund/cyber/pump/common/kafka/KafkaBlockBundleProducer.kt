package fund.cyber.pump.common.kafka

import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.search.model.events.PumpEvent

interface KafkaBlockBundleProducer<in T : BlockBundle> {

    fun storeBlockBundle(blockBundleEvents: List<Pair<PumpEvent, T>>)
}
