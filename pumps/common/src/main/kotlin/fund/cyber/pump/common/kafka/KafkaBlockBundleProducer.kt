package fund.cyber.pump.common.kafka

import fund.cyber.pump.common.node.BlockBundle

interface KafkaBlockBundleProducer<in T : BlockBundle> {

    fun storeBlockBundle(blockBundles: List<T>)
}