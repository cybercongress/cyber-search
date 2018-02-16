package fund.cyber.cassandra.bitcoin.configuration

import fund.cyber.cassandra.configuration.CassandraRepositoriesConfiguration
import fund.cyber.cassandra.migration.MigrationSettings
import fund.cyber.search.configuration.*
import fund.cyber.search.model.chains.BitcoinFamilyChain
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.*
import org.springframework.core.type.AnnotatedTypeMetadata
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories


private val BitcoinFamilyChain.keyspace: String get() = name.toLowerCase()

@Configuration
@EnableReactiveCassandraRepositories(basePackages = ["fund.cyber.cassandra.bitcoin.repository"])
@Conditional(BitcoinFamilyChainCondition::class)
open class BitcoinRepositoryConfiguration(
        @Value("#{systemProperties['${CASSANDRA_HOSTS}'] ?: '${CASSANDRA_HOSTS_DEFAULT}'}")
        private val cassandraHosts: String,
        @Value("#{systemProperties['${CASSANDRA_PORT}'] ?: '${CASSANDRA_PORT_DEFAULT}'}")
        private val cassandraPort: Int
) : CassandraRepositoriesConfiguration(cassandraHosts, cassandraPort) {

    private val chain = BitcoinFamilyChain.valueOf(env(CHAIN, ""))

    override fun getKeyspaceName(): String = chain.keyspace
    override fun getEntityBasePackages(): Array<String> = arrayOf("fund.cyber.cassandra.bitcoin.model")

    @Bean
    open fun migrationSettings(): MigrationSettings {
        return BitcoinMigrationSettings(chain)
    }
}


private class BitcoinFamilyChainCondition : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {

        val chain = context.environment.getProperty(CHAIN) ?: ""
        return BitcoinFamilyChain.values().map(BitcoinFamilyChain::name).contains(chain)
    }
}