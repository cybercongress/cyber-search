package fund.cyber.cassandra.common

import fund.cyber.cassandra.migration.DefaultMigrationsLoader
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.support.GenericApplicationContext


@Configuration
class CassandraServiceCommonConfiguration {
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
    fun migrationsLoader(resourceLoader: GenericApplicationContext) = DefaultMigrationsLoader(
        resourceLoader = resourceLoader
    )
}
