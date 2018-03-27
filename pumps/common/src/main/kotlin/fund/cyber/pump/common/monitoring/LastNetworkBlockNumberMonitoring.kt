package fund.cyber.pump.common.monitoring

import fund.cyber.pump.common.node.FlowableBlockchainInterface
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

private val log = LoggerFactory.getLogger(LastNetworkBlockNumberMonitoring::class.java)!!

private const val LAST_NETWORK_BLOCK_NUMBER_TIMEOUT = 10 * 1000L

@Component
class LastNetworkBlockNumberMonitoring(
        monitoring: MeterRegistry,
        private val blockchainInterface: FlowableBlockchainInterface<*>
) {

    private val lastProcessedBlockMonitor = monitoring
            .gauge("pump_last_network_block", AtomicLong(blockchainInterface.lastNetworkBlock()))!!

    @Scheduled(fixedRate = LAST_NETWORK_BLOCK_NUMBER_TIMEOUT)
    fun getLastNetworkBlockNumber() {
        try {
            val lastNetworkBlock = blockchainInterface.lastNetworkBlock()
            lastProcessedBlockMonitor.set(lastNetworkBlock)
        } catch (e: Exception) {
            log.error("Error getting network block number", e)
        }
    }
}
