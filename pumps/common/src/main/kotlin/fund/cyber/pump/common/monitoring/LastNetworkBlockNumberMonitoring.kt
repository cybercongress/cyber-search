package fund.cyber.pump.common.monitoring

import fund.cyber.pump.common.node.FlowableBlockchainInterface
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong


@Component
class LastNetworkBlockNumberMonitoring(
        private val monitoring: MeterRegistry,
        private val blockchainInterface: FlowableBlockchainInterface<*>
) {

    private val lastProcessedBlockMonitor = monitoring
            .gauge("pump_last_network_block", AtomicLong(blockchainInterface.lastNetworkBlock()))!!

    @Scheduled(fixedRate = 10 * 1000)
    fun getLastNetworkBlockNumber() {
        try {
            val lastNetworkBlock = blockchainInterface.lastNetworkBlock()
            lastProcessedBlockMonitor.set(lastNetworkBlock)
        } catch (e: Exception) {}
    }
}