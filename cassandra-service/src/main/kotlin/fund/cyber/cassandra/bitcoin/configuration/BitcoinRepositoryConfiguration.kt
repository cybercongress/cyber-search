package fund.cyber.cassandra.bitcoin.configuration

import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.PoolingOptions
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.BitcoinFamilyChain
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.*
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer
import org.springframework.core.type.AnnotatedTypeMetadata
import org.springframework.data.cassandra.config.AbstractReactiveCassandraConfiguration
import org.springframework.data.cassandra.config.SchemaAction
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories


const val MAX_CONCURRENT_REQUESTS = 8182
const val PREFERRED_CONCURRENT_REQUEST_TO_SAVE_ENTITIES_LIST = MAX_CONCURRENT_REQUESTS / 8

private val BitcoinFamilyChain.keyspace: String get() = name.toLowerCase()

@Configuration
@EnableReactiveCassandraRepositories(basePackages = ["fund.cyber.cassandra.bitcoin.repository"])
@Conditional(BitcoinFamilyChainCondition::class)
open class BitcoinRepositoryConfiguration : AbstractReactiveCassandraConfiguration() {

    private val chain = BitcoinFamilyChain.valueOf(env(CHAIN, ""))

    override fun getKeyspaceName(): String = chain.keyspace
    override fun getEntityBasePackages(): Array<String> = arrayOf("fund.cyber.cassandra.bitcoin.model")

    override fun getPoolingOptions() = PoolingOptions()
            .setMaxRequestsPerConnection(HostDistance.LOCAL, MAX_CONCURRENT_REQUESTS)
            .setMaxRequestsPerConnection(HostDistance.REMOTE, MAX_CONCURRENT_REQUESTS)!!
}


private class BitcoinFamilyChainCondition : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {

        val chain = context.environment.getProperty(CHAIN) ?: ""
        return BitcoinFamilyChain.values().map(BitcoinFamilyChain::name).contains(chain)
    }
}