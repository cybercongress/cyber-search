package fund.cyber.pump.common

import fund.cyber.common.StackCache
import fund.cyber.pump.common.kafka.KafkaBlockBundleProducer
import fund.cyber.pump.common.kafka.LastPumpedBundlesProvider
import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.pump.common.node.BlockBundleEventGenerator
import fund.cyber.pump.common.node.FlowableBlockchainInterface
import fund.cyber.search.configuration.STACK_CACHE_SIZE
import fund.cyber.search.configuration.STACK_CACHE_SIZE_DEFAULT
import fund.cyber.search.configuration.START_BLOCK_NUMBER
import fund.cyber.search.model.events.PumpEvent
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.reactivex.rxkotlin.toFlowable
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong


private val log = LoggerFactory.getLogger(ChainPump::class.java)!!

private const val BLOCK_BUFFER_TIMESPAN = 3L

@Component
@DependsOn(value = ["kafkaBlockBundleProducer"])  // to resolve generics at runtime
class ChainPump<T : BlockBundle>(
        private val flowableBlockchainInterface: FlowableBlockchainInterface<T>,
        private val kafkaBlockBundleProducer: KafkaBlockBundleProducer<T>,
        private val lastPumpedBundlesProvider: LastPumpedBundlesProvider<T>,
        private val monitoring: MeterRegistry,
        @Value("\${$STACK_CACHE_SIZE:$STACK_CACHE_SIZE_DEFAULT}")
        private val stackCacheSize: Int,
        private val blockBundleEventGenerator: BlockBundleEventGenerator<T>,
        private val applicationContext: ConfigurableApplicationContext
) {

    fun startPump() {

        val startBlockNumber = startBlockNumber()

        log.info("Start block number is $startBlockNumber")
        initializeStreamProcessing(startBlockNumber)
    }


    private fun initializeStreamProcessing(startBlockNumber: Long) {

        val lastPumpedBlockNumber = AtomicLong(startBlockNumber - 1)

        val lastProcessedBlockMonitor = monitoring.gauge("pump_last_processed_block", lastPumpedBlockNumber)!!
        val blockSizeMonitor = DistributionSummary.builder("pump_block_size").baseUnit("bytes").register(monitoring)
        val kafkaWriteMonitor = monitoring.timer("pump_bundle_kafka_store")

        val history = initializeStackCache()

        flowableBlockchainInterface.subscribeBlocks(startBlockNumber)
                .flatMap { blockBundle -> blockBundleEventGenerator.generate(blockBundle, history).toFlowable() }
                .buffer(BLOCK_BUFFER_TIMESPAN, TimeUnit.SECONDS)
                .blockingSubscribe(
                        { blockBundleEvents ->
                            if (blockBundleEvents.isEmpty()) return@blockingSubscribe
                            blockBundleEvents.forEach { event ->
                                val bundle = event.second
                                lastProcessedBlockMonitor.set(bundle.number)
                                blockSizeMonitor.record(bundle.blockSize.toDouble())
                            }
                            val blocksToWrite = blockBundleEvents.filter { e -> e.first == PumpEvent.NEW_BLOCK }
                                    .map { e -> e.second }
                            log.trace("Writing ${blocksToWrite.first().number}-${blocksToWrite.last().number} blocks")
                            kafkaWriteMonitor.recordCallable {
                                kafkaBlockBundleProducer.storeBlockBundle(blockBundleEvents)
                            }
                        },
                        { error ->
                            log.error("Error during processing stream", error)
                            log.info("Closing application context...")
                            applicationContext.close()
                        }
                )
    }

    private fun initializeStackCache() = StackCache<T>(stackCacheSize)

    private fun startBlockNumber(): Long {
        return System.getenv(START_BLOCK_NUMBER)?.toLong() ?: (lastPumpedBlockNumber()?.plus(1) ?: 0)
    }

    private fun lastPumpedBlockNumber() = lastPumpedBundlesProvider.getLastBlockBundles().firstOrNull()?.second?.number

}
