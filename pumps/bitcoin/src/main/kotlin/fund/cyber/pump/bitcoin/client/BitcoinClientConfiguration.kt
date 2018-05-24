package fund.cyber.pump.bitcoin.client

import fund.cyber.search.configuration.BITCOIN_TX_OUTS_CACHE_HEAP_SIZE
import fund.cyber.search.configuration.BITCOIN_TX_OUTS_CACHE_HEAP_SIZE_DEFAULT
import fund.cyber.search.model.bitcoin.BitcoinCacheTxOutput
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.ehcache.Cache
import org.ehcache.CacheManager
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder
import org.ehcache.config.units.MemoryUnit
import org.ehcache.core.spi.service.StatisticsService
import org.ehcache.impl.internal.statistics.DefaultStatisticsService
import org.springframework.beans.factory.annotation.Value

const val MAX_PER_ROUTE = 16
const val MAX_TOTAL = 32

const val TXS_OUTPUTS_CACHE_NAME = "bitcoin.tx.outputs"
const val MEMPOOL_HASHES_CASH = "bitcoin.mempool.hashes"
const val MEMPOOL_HASHES_CASH_SIZE_MB = 300L

@Configuration
class BitcoinClientConfiguration(
    @Value("\${$BITCOIN_TX_OUTS_CACHE_HEAP_SIZE:$BITCOIN_TX_OUTS_CACHE_HEAP_SIZE_DEFAULT}")
    private val txOutsCacheHeapSize: Long
) {

    private val defaultHttpHeaders = listOf(BasicHeader("Keep-Alive", "timeout=10, max=1024"))
    private val connectionManager = PoolingHttpClientConnectionManager().apply {
        defaultMaxPerRoute = MAX_PER_ROUTE
        maxTotal = MAX_TOTAL
    }

    @Bean
    fun httpClient() = HttpClients.custom()
            .setConnectionManager(connectionManager)
            .setConnectionManagerShared(true)
            .setDefaultHeaders(defaultHttpHeaders)
            .build()!!

    @Bean
    fun txOutputCache(cacheManager: CacheManager): Cache<String, BitcoinCacheTxOutput> {
        return cacheManager.getCache(TXS_OUTPUTS_CACHE_NAME, String::class.java, BitcoinCacheTxOutput::class.java)
    }

    @Bean
    fun mempoolHashesCache(cacheManager: CacheManager): Cache<String, String> {
        return cacheManager.getCache(MEMPOOL_HASHES_CASH, String::class.java, String::class.java)
    }

    @Bean
    fun cacheStatisticsService() = DefaultStatisticsService()

    @Bean
    fun cacheManager(cacheStatisticsService: StatisticsService): CacheManager {

        return newCacheManagerBuilder()
            .withCache(TXS_OUTPUTS_CACHE_NAME,
                newCacheConfigurationBuilder(
                    String::class.java,
                    BitcoinCacheTxOutput::class.java,
                    newResourcePoolsBuilder().heap(txOutsCacheHeapSize, MemoryUnit.GB)
                )
            )
            .withCache(MEMPOOL_HASHES_CASH,
                newCacheConfigurationBuilder(
                    String::class.java,
                    String::class.java,
                    newResourcePoolsBuilder().heap(MEMPOOL_HASHES_CASH_SIZE_MB, MemoryUnit.MB)
                ))
            .using(cacheStatisticsService)
            .build(true)
    }
}
