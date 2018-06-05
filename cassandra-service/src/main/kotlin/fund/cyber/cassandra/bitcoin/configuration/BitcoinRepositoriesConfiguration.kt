package fund.cyber.cassandra.bitcoin.configuration

import com.datastax.driver.extras.codecs.jdk8.InstantCodec
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractSummaryRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinBlockTxRepository
import fund.cyber.cassandra.common.NoChainCondition
import fund.cyber.cassandra.common.RoutingReactiveCassandraRepositoryFactoryBean
import fund.cyber.cassandra.common.defaultKeyspaceSpecification
import fund.cyber.cassandra.common.CassandraRepositoriesConfiguration
import fund.cyber.cassandra.common.SearchRepositoriesConfiguration
import fund.cyber.cassandra.common.keyspace
import fund.cyber.cassandra.migration.BlockchainMigrationSettings
import fund.cyber.cassandra.migration.MigrationSettings
import fund.cyber.search.configuration.CASSANDRA_HOSTS
import fund.cyber.search.configuration.CASSANDRA_HOSTS_DEFAULT
import fund.cyber.search.configuration.CASSANDRA_MAX_CONNECTIONS_LOCAL
import fund.cyber.search.configuration.CASSANDRA_MAX_CONNECTIONS_LOCAL_DEFAULT
import fund.cyber.search.configuration.CASSANDRA_MAX_CONNECTIONS_REMOTE
import fund.cyber.search.configuration.CASSANDRA_MAX_CONNECTIONS_REMOTE_DEFAULT
import fund.cyber.search.configuration.CASSANDRA_PORT
import fund.cyber.search.configuration.CASSANDRA_PORT_DEFAULT
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.chains.Chain
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Condition
import org.springframework.context.annotation.ConditionContext
import org.springframework.context.annotation.Conditional
import org.springframework.context.annotation.Configuration
import org.springframework.core.type.AnnotatedTypeMetadata
import org.springframework.data.cassandra.ReactiveSession
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean
import org.springframework.data.cassandra.config.ClusterBuilderConfigurer
import org.springframework.data.cassandra.core.ReactiveCassandraOperations
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories
import org.springframework.stereotype.Component


private class BitcoinFamilyChainCondition : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {

        val chain = context.environment.getProperty(CHAIN) ?: ""
        return BitcoinFamilyChain.values().map(BitcoinFamilyChain::name).contains(chain)
    }
}


@Configuration
@EnableReactiveCassandraRepositories(
    basePackages = ["fund.cyber.cassandra.bitcoin.repository"],
    reactiveCassandraTemplateRef = "bitcoinCassandraTemplate",
    repositoryFactoryBeanClass = RoutingReactiveCassandraRepositoryFactoryBean::class
)
@Conditional(BitcoinFamilyChainCondition::class)
class BitcoinRepositoriesConfiguration(
    @Value("\${$CASSANDRA_HOSTS:$CASSANDRA_HOSTS_DEFAULT}")
    private val cassandraHosts: String,
    @Value("\${$CASSANDRA_PORT:$CASSANDRA_PORT_DEFAULT}")
    private val cassandraPort: Int,
    @Value("\${$CASSANDRA_MAX_CONNECTIONS_LOCAL:$CASSANDRA_MAX_CONNECTIONS_LOCAL_DEFAULT}")
    private val maxConnectionsLocal: Int,
    @Value("\${$CASSANDRA_MAX_CONNECTIONS_REMOTE:$CASSANDRA_MAX_CONNECTIONS_REMOTE_DEFAULT}")
    private val maxConnectionsRemote: Int
) : CassandraRepositoriesConfiguration(cassandraHosts, cassandraPort, maxConnectionsLocal, maxConnectionsRemote) {

    override fun getKeyspaceName(): String = chain().keyspace
    override fun getEntityBasePackages(): Array<String> = arrayOf("fund.cyber.cassandra.bitcoin.model")

    override fun getKeyspaceCreations(): List<CreateKeyspaceSpecification> {
        return super.getKeyspaceCreations() + listOf(defaultKeyspaceSpecification(chain().lowerCaseName))
    }

    @Bean
    fun chain(): Chain = BitcoinFamilyChain.valueOf(env(CHAIN, ""))

    @Bean
    fun migrationSettings(): MigrationSettings {
        return BlockchainMigrationSettings(chain())
    }

    @Bean("bitcoinCassandraTemplate")
    fun reactiveCassandraTemplate(
        @Qualifier("bitcoinReactiveSession") session: ReactiveSession
    ): ReactiveCassandraOperations {
        return ReactiveCassandraTemplate(DefaultReactiveSessionFactory(session), cassandraConverter())
    }

    @Bean("bitcoinReactiveSession")
    fun reactiveSession(
        @Qualifier("bitcoinSession") session: CassandraSessionFactoryBean
    ): ReactiveSession {
        return DefaultBridgedReactiveSession(session.`object`)
    }

    override fun getClusterBuilderConfigurer(): ClusterBuilderConfigurer? {
        return ClusterBuilderConfigurer { clusterBuilder ->
            clusterBuilder.configuration.codecRegistry.register(InstantCodec.instance)
            return@ClusterBuilderConfigurer clusterBuilder
        }
    }

    @Bean("bitcoinSession")
    override fun session(): CassandraSessionFactoryBean {
        val session = super.session()
        session.setKeyspaceName(keyspaceName)
        return session
    }
}


@Component("bitcoin-search-repositories")
@Conditional(NoChainCondition::class)
class BitcoinSearchRepositoriesConfiguration : SearchRepositoriesConfiguration(
    entityBasePackage = "fund.cyber.cassandra.bitcoin.model", keyspacePrefix = "bitcoin"
) {

    override fun repositoriesClasses(): List<Class<*>> = listOf(
        BitcoinBlockRepository::class.java,
        PageableBitcoinBlockTxRepository::class.java,
        BitcoinTxRepository::class.java,
        BitcoinContractSummaryRepository::class.java,
        BitcoinContractTxRepository::class.java,
        PageableBitcoinContractTxRepository::class.java,
        PageableBitcoinContractMinedBlockRepository::class.java
    )

    override fun customConversions() = CassandraCustomConversions(emptyList<Any>())

}
