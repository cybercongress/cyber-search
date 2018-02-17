package fund.cyber.pump.common

import fund.cyber.search.model.chains.Chain
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong


private val log = LoggerFactory.getLogger(ChainPump::class.java)!!


//todo add chain reorganisation
@Component
class ChainPump<T : BlockBundle>(
        private val flowableBlockchainInterface: FlowableBlockchainInterface<T>,
        private val kafkaBlockBundleProducer: KafkaBlockBundleProducer<T>,
        private val lastPumpedBundlesProvider: LastPumpedBundlesProvider<T>,
        private val chain: Chain,
        private val monitoring: MeterRegistry
) {

    fun startPump() {

        val lastPumpedBlockNumber = lastPumpedBundlesProvider.getLastBlockBundles().firstOrNull()?.second?.number ?: 0
        val startBlockNumber = lastPumpedBlockNumber + 1

        log.info("Start block number is $startBlockNumber")
        initializeStreamProcessing(startBlockNumber)
    }


    private fun initializeStreamProcessing(startBlockNumber: Long) {

        val labels = listOf(Tag.of("chain", chain.name))
        val lastProcessedBlockMonitor = monitoring.gauge("pump_last_processed_block", labels, AtomicLong(startBlockNumber - 1))!!

        flowableBlockchainInterface.subscribeBlocks(startBlockNumber)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.io())
                .blockingSubscribe(
                        { blockBundle ->
                            log.info("Processing ${blockBundle.number} block")
                            lastProcessedBlockMonitor.set(blockBundle.number)
                            kafkaBlockBundleProducer.storeBlockBundle(blockBundle)
                        },
                        { error ->
                            if (error !is ChainReindexationException) {
                                log.error("Error during processing stream", error)
                                //close context
                            }
                        }
                )
    }

    class ChainReindexationException : RuntimeException()
}