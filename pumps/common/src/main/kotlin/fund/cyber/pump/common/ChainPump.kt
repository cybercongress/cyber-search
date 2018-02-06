package fund.cyber.pump.common

import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component


private val log = LoggerFactory.getLogger(ChainPump::class.java)!!


//todo add chain reorganisation
@Component
class ChainPump<T : BlockBundle>(
        private val flowableBlockchainInterface: FlowableBlockchainInterface<T>,
        private val kafkaBlockBundleProducer: KafkaBlockBundleProducer<T>,
        private val lastPumpedBundlesProvider: LastPumpedBundlesProvider<T>

) {

    fun startPump() {

        val lastPumpedBlockNumber = lastPumpedBundlesProvider.getLastBlockBundles().firstOrNull()?.second?.number ?: 0
        val startBlockNumber = lastPumpedBlockNumber + 1

        log.info("Start block number is $startBlockNumber")
        initializeStreamProcessing(startBlockNumber)
    }


    private fun initializeStreamProcessing(startBlockNumber: Long) {

        flowableBlockchainInterface.subscribeBlocks(startBlockNumber)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.io())
                .blockingSubscribe(
                        { blockBundle ->
                            log.info("Processing ${blockBundle.number} block")
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