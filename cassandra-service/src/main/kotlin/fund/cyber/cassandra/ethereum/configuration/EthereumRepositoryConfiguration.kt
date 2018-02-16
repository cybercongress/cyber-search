package fund.cyber.cassandra.ethereum.configuration

import fund.cyber.cassandra.configuration.CassandraRepositoriesConfiguration
import fund.cyber.cassandra.configuration.keyspace
import fund.cyber.cassandra.migration.BlockchainMigrationSettings
import fund.cyber.cassandra.migration.MigrationSettings
import fund.cyber.search.configuration.*
import fund.cyber.search.model.chains.EthereumFamilyChain
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.*
import org.springframework.core.type.AnnotatedTypeMetadata
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories


@Configuration
@EnableReactiveCassandraRepositories(basePackages = ["fund.cyber.cassandra.ethereum.repository"])
@Conditional(EthereumFamilyChainCondition::class)
open class EthereumRepositoryConfiguration(
        @Value("#{systemProperties['${CASSANDRA_HOSTS}'] ?: '${CASSANDRA_HOSTS_DEFAULT}'}")
        private val cassandraHosts: String,
        @Value("#{systemProperties['${CASSANDRA_PORT}'] ?: '${CASSANDRA_PORT_DEFAULT}'}")
        private val cassandraPort: Int
) : CassandraRepositoriesConfiguration(cassandraHosts, cassandraPort) {

    private val chain = EthereumFamilyChain.valueOf(env(CHAIN, ""))

    override fun getKeyspaceName(): String = chain.keyspace
    override fun getEntityBasePackages(): Array<String> = arrayOf("fund.cyber.cassandra.ethereum.model")

    @Bean
    open fun migrationSettings(): MigrationSettings {
        return BlockchainMigrationSettings(chain)
    }
}


private class EthereumFamilyChainCondition : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {

        val chain = context.environment.getProperty(CHAIN) ?: ""
        return EthereumFamilyChain.values().map(EthereumFamilyChain::name).contains(chain)
    }
}