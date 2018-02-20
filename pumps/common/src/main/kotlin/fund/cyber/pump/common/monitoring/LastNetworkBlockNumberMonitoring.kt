package fund.cyber.pump.common.monitoring

import fund.cyber.pump.common.FlowableBlockchainInterface
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong


@EnableScheduling
@Configuration
class MonitoringConfiguration


@Component
class LastNetworkBlockNumberMonitoring(
        monitoring: MonitoringService,
        private val blockchainInterface: FlowableBlockchainInterface<*>
) {

    val lastProcessedBlockMonitor = monitoring
            .gauge("pump_last_network_block", AtomicLong(blockchainInterface.lastNetworkBlock()))

    @Scheduled(fixedRate = 10 * 1000)
    fun getLastNetworkBlockNumber() {
        try {
            val lastNetworkBlock = blockchainInterface.lastNetworkBlock()
            lastProcessedBlockMonitor.set(lastNetworkBlock)
        } catch (e: Exception) {}
    }
}