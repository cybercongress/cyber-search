package fund.cyber.cassandra.bitcoin.configuration

import fund.cyber.cassandra.configuration.CassandraRepositoriesConfiguration
import fund.cyber.cassandra.configuration.keyspace
import fund.cyber.cassandra.migration.BlockchainMigrationSettings
import fund.cyber.cassandra.migration.MigrationSettings
import fund.cyber.search.configuration.CASSANDRA_HOSTS
import fund.cyber.search.configuration.CASSANDRA_HOSTS_DEFAULT
import fund.cyber.search.configuration.CASSANDRA_PORT
import fund.cyber.search.configuration.CASSANDRA_PORT_DEFAULT
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.BitcoinFamilyChain
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Condition
import org.springframework.context.annotation.ConditionContext
import org.springframework.context.annotation.Conditional
import org.springframework.context.annotation.Configuration
import org.springframework.core.type.AnnotatedTypeMetadata
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories


@Configuration
@EnableReactiveCassandraRepositories(basePackages = ["fund.cyber.cassandra.bitcoin.repository"])
@Conditional(BitcoinFamilyChainCondition::class)
class BitcoinRepositoryConfiguration(
        @Value("\${$CASSANDRA_HOSTS:$CASSANDRA_HOSTS_DEFAULT}")
        private val cassandraHosts: String,
        @Value("\${$CASSANDRA_PORT:$CASSANDRA_PORT_DEFAULT}")
        private val cassandraPort: Int
) : CassandraRepositoriesConfiguration(cassandraHosts, cassandraPort) {

    private val chain = BitcoinFamilyChain.valueOf(env(CHAIN, ""))

    override fun getKeyspaceName(): String = chain.keyspace
    override fun getEntityBasePackages(): Array<String> = arrayOf("fund.cyber.cassandra.bitcoin.model")

    @Bean
    fun migrationSettings(): MigrationSettings {
        return BlockchainMigrationSettings(chain)
    }
}


private class BitcoinFamilyChainCondition : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {

        val chain = context.environment.getProperty(CHAIN) ?: ""
        return BitcoinFamilyChain.values().map(BitcoinFamilyChain::name).contains(chain)
    }
}
