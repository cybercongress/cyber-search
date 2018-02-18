package fund.cyber.pump.common.genesis

import fund.cyber.pump.common.BlockBundle
import fund.cyber.pump.common.KafkaBlockBundleProducer
import fund.cyber.search.model.chains.Chain
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

private val log = LoggerFactory.getLogger(GenesisLoader::class.java)!!

@Component
class GenesisLoader<T : BlockBundle>(
        private val chain: Chain,
        private val bundleProvider: GenesisBundleProvider<T>,
        private val kafkaBlockBundleProducer: KafkaBlockBundleProducer<T>
) {

    fun load() {
        log.info("Start loading genesis info for ${chain.name}!")
        kafkaBlockBundleProducer.storeBlockBundle(bundleProvider.provide(chain))
        log.info("Loading genesis info for ${chain.name} completed!")
    }
}