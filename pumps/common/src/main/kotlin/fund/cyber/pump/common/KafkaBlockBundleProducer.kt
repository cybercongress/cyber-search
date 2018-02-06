package fund.cyber.pump.common

interface KafkaBlockBundleProducer<in T : BlockBundle> {

    fun storeBlockBundle(blockBundle: T)
}