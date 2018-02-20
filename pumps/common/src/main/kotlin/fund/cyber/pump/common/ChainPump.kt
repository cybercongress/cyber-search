package fund.cyber.pump.common

import fund.cyber.pump.common.monitoring.MonitoringService
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong


private val log = LoggerFactory.getLogger(ChainPump::class.java)!!


//todo add chain reorganisation
@Component
@DependsOn(value = ["kafkaBlockBundleProducer"])  // to resolve generics at runtime
class ChainPump<T : BlockBundle>(
        private val flowableBlockchainInterface: FlowableBlockchainInterface<T>,
        private val kafkaBlockBundleProducer: KafkaBlockBundleProducer<T>,
        private val lastPumpedBundlesProvider: LastPumpedBundlesProvider<T>,
        private val monitoring: MonitoringService
) {

    fun startPump() {

        val lastPumpedBlockNumber = lastPumpedBundlesProvider.getLastBlockBundles().firstOrNull()?.second?.number ?: -1
        val startBlockNumber = lastPumpedBlockNumber + 1

        log.info("Start block number is $startBlockNumber")
        initializeStreamProcessing(startBlockNumber)
    }


    private fun initializeStreamProcessing(startBlockNumber: Long) {

        val lastProcessedBlockMonitor = monitoring.gauge("pump_last_processed_block", AtomicLong(startBlockNumber - 1))
        val blockSizeMonitor = monitoring.summary("pump_block_size", "bytes")

        flowableBlockchainInterface.subscribeBlocks(startBlockNumber)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.io())
                .blockingSubscribe(
                        { blockBundle ->
                            log.trace("Processing ${blockBundle.number} block")
                            lastProcessedBlockMonitor.set(blockBundle.number)
                            blockSizeMonitor.record(blockBundle.blockSize.toDouble())
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