package fund.cyber.pump.bitcoin.client

import fund.cyber.search.model.bitcoin.BitcoinCacheTx
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.ehcache.Cache
import org.ehcache.CacheManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder.heap
import org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder

const val MAX_PER_ROUTE = 16
const val MAX_TOTAL = 32
const val EHCACHE_HEAP_ENTRIES = 20000000L

@Configuration
class BitcoinClientConfiguration {

    @Autowired
    private lateinit var chain: BitcoinFamilyChain

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
    fun kafkaTopicNames(): List<String> = listOf(chain.txPumpTopic, chain.blockPumpTopic)

    @Bean
    fun txCache(
        cacheManager: CacheManager
    ): Cache<String, BitcoinCacheTx> {
        return cacheManager.getCache("bitcoin.transactions", String::class.java, BitcoinCacheTx::class.java)
    }

    @Bean
    fun cacheManager(): CacheManager {
        return newCacheManagerBuilder()
            .withCache("bitcoin.transactions",
                newCacheConfigurationBuilder(
                    String::class.java,
                    BitcoinCacheTx::class.java,
                    heap(EHCACHE_HEAP_ENTRIES)
                )
            )
            .build(true)
    }
}
