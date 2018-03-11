package fund.cyber.pump.common

import fund.cyber.pump.common.kafka.KafkaBlockBundleProducer
import fund.cyber.pump.common.kafka.LastPumpedBundlesProvider
import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.pump.common.node.FlowableBlockchainInterface
import fund.cyber.search.configuration.START_BLOCK_NUMBER
import fund.cyber.search.configuration.START_BLOCK_NUMBER_DEFAULT
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong


private val log = LoggerFactory.getLogger(ChainPump::class.java)!!


//todo add chain reorganisation
@Component
@DependsOn(value = ["kafkaBlockBundleProducer"])  // to resolve generics at runtime
class ChainPump<T : BlockBundle>(
        private val flowableBlockchainInterface: FlowableBlockchainInterface<T>,
        private val kafkaBlockBundleProducer: KafkaBlockBundleProducer<T>,
        private val lastPumpedBundlesProvider: LastPumpedBundlesProvider<T>,
        private val monitoring: MeterRegistry,
        @Value("\${$START_BLOCK_NUMBER:$START_BLOCK_NUMBER_DEFAULT}")
        private val startBlockNumber: Long
) {

    fun startPump() {

        val lastPumpedBlockNumber = lastBlockNumber()
        val startBlockNumber = lastPumpedBlockNumber + 1

        log.info("Start block number is $startBlockNumber")
        initializeStreamProcessing(startBlockNumber)
    }


    private fun initializeStreamProcessing(startBlockNumber: Long) {

        val lastPumpedBlockNumber = AtomicLong(startBlockNumber - 1)

        val lastProcessedBlockMonitor = monitoring.gauge("pump_last_processed_block", lastPumpedBlockNumber)!!
        val blockSizeMonitor = DistributionSummary.builder("pump_block_size").baseUnit("bytes").register(monitoring)
        val kafkaWriteMonitor = monitoring.timer("pump_bundle_kafka_store")

        flowableBlockchainInterface.subscribeBlocks(startBlockNumber)
                .buffer(3, TimeUnit.SECONDS)
                .blockingSubscribe(
                        { blockBundles ->
                            if (blockBundles.isEmpty()) return@blockingSubscribe
                            blockBundles.forEach { bundle ->
                                lastProcessedBlockMonitor.set(bundle.number)
                                blockSizeMonitor.record(bundle.blockSize.toDouble())
                            }
                            log.trace("Writing ${blockBundles.first().number}-${blockBundles.last().number} blocks")
                            kafkaWriteMonitor.recordCallable { kafkaBlockBundleProducer.storeBlockBundle(blockBundles) }
                        },
                        { error ->
                            if (error !is ChainReindexationException) {
                                log.error("Error during processing stream", error)
                                //close context
                            }
                        }
                )
    }

    private fun lastBlockNumber(): Long {
        return if (startBlockNumber == START_BLOCK_NUMBER_DEFAULT)
            lastPumpedBundlesProvider.getLastBlockBundles().firstOrNull()?.second?.number ?: -1
        else
            startBlockNumber
    }

    class ChainReindexationException : RuntimeException()
}