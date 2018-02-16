package fund.cyber.cassandra.configuration

import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.PoolingOptions
import fund.cyber.cassandra.migration.DefaultMigrationsLoader
import fund.cyber.search.model.chains.BitcoinFamilyChain
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.cassandra.ReactiveSession
import org.springframework.data.cassandra.config.AbstractReactiveCassandraConfiguration


const val MAX_CONCURRENT_REQUESTS = 8182

private val BitcoinFamilyChain.keyspace: String get() = name.toLowerCase()


abstract class CassandraRepositoriesConfiguration(
        private val cassandraHosts: String,
        private val cassandraPort: Int
) : AbstractReactiveCassandraConfiguration() {

    override fun getPoolingOptions() = PoolingOptions()
            .setMaxRequestsPerConnection(HostDistance.LOCAL, MAX_CONCURRENT_REQUESTS)
            .setMaxRequestsPerConnection(HostDistance.REMOTE, MAX_CONCURRENT_REQUESTS)!!

    override fun getPort() = cassandraPort
    override fun getContactPoints() = cassandraHosts

    @Bean
    @Primary
    override fun reactiveSession(): ReactiveSession {
        return super.reactiveSession()
    }
}


@Configuration
open class CassandraConfiguration {
    private val defaultHttpHeaders = listOf(BasicHeader("Keep-Alive", "timeout=10, max=1024"))
    private val connectionManager = PoolingHttpClientConnectionManager().apply {
        defaultMaxPerRoute = 16
        maxTotal = 32
    }

    @Bean
    open fun httpClient() = HttpClients.custom()
            .setConnectionManager(connectionManager)
            .setConnectionManagerShared(true)
            .setDefaultHeaders(defaultHttpHeaders)
            .build()!!

    @Bean
    open fun migrationsLoader() = DefaultMigrationsLoader()
}