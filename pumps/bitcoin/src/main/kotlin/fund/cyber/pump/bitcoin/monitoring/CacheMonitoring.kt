package fund.cyber.pump.bitcoin.monitoring

import fund.cyber.pump.bitcoin.client.TXS_OUTPUTS_CACHE_NAME
import io.micrometer.core.instrument.MeterRegistry
import org.ehcache.core.spi.service.StatisticsService
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

private const val CACHE_STATS_TIMEOUT = 10 * 1000L

@Component
class CacheMonitoring(
    monitoring: MeterRegistry,
    private val cacheStatisticsService: StatisticsService
) {

    private val cacheSizeMonitor = monitoring.gauge("txs_outputs_cache_size_bytes", AtomicLong(0))!!
    private val cacheItemsMonitor = monitoring.gauge("txs_outputs_cache_items", AtomicLong(0))!!

    @Scheduled(fixedRate = CACHE_STATS_TIMEOUT)
    fun cacheStats() {

        val stats = cacheStatisticsService.getCacheStatistics(TXS_OUTPUTS_CACHE_NAME)
        cacheSizeMonitor.set(stats.tierStatistics["OnHeap"]!!.occupiedByteSize)
        cacheItemsMonitor.set(stats.tierStatistics["OnHeap"]!!.mappings)
    }

}
