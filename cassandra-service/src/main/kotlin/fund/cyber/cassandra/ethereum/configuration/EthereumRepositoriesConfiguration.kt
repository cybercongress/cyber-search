package fund.cyber.cassandra.ethereum.configuration

import com.datastax.driver.extras.codecs.jdk8.InstantCodec
import fund.cyber.cassandra.common.CassandraRepositoriesConfiguration
import fund.cyber.cassandra.common.NoChainCondition
import fund.cyber.cassandra.common.RoutingReactiveCassandraRepositoryFactoryBean
import fund.cyber.cassandra.common.SearchRepositoriesConfiguration
import fund.cyber.cassandra.common.defaultKeyspaceSpecification
import fund.cyber.cassandra.common.keyspace
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumBlockTxRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedUncleRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractTxRepository
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
import fund.cyber.search.configuration.CHAIN_FAMILY
import fund.cyber.search.configuration.CHAIN_NAME
import fund.cyber.search.jsonDeserializer
import fund.cyber.search.jsonSerializer
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
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
import org.springframework.data.convert.CustomConversions
import org.springframework.stereotype.Component


private class EthereumFamilyChainCondition : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {

        val chain = context.environment.getProperty(CHAIN_FAMILY) ?: ""
        return chain == ChainFamily.ETHEREUM.toString()
    }
}


@Configuration
@EnableReactiveCassandraRepositories(
    basePackages = ["fund.cyber.cassandra.ethereum.repository"],
    reactiveCassandraTemplateRef = "ethereumCassandraTemplate",
    repositoryFactoryBeanClass = RoutingReactiveCassandraRepositoryFactoryBean::class
)
@Conditional(EthereumFamilyChainCondition::class)
class EthereumRepositoriesConfiguration(
    @Value("\${$CASSANDRA_HOSTS:$CASSANDRA_HOSTS_DEFAULT}")
    private val cassandraHosts: String,
    @Value("\${$CASSANDRA_PORT:$CASSANDRA_PORT_DEFAULT}")
    private val cassandraPort: Int,
    @Value("\${$CASSANDRA_MAX_CONNECTIONS_LOCAL:$CASSANDRA_MAX_CONNECTIONS_LOCAL_DEFAULT}")
    private val maxConnectionsLocal: Int,
    @Value("\${$CASSANDRA_MAX_CONNECTIONS_REMOTE:$CASSANDRA_MAX_CONNECTIONS_REMOTE_DEFAULT}")
    private val maxConnectionsRemote: Int
) : CassandraRepositoriesConfiguration(cassandraHosts, cassandraPort, maxConnectionsLocal, maxConnectionsRemote) {


    @Value("\${$CHAIN_FAMILY:}")
    private lateinit var chainFamily: String

    @Value("\${$CHAIN_NAME:}")
    private lateinit var chainName: String

    @Bean
    fun chainInfo() = ChainInfo(
        ChainFamily.valueOf(chainFamily),
        if (chainName.isEmpty()) chainFamily else chainName
    )

    override fun getKeyspaceName(): String = chainInfo().keyspace
    override fun getEntityBasePackages(): Array<String> = arrayOf("fund.cyber.cassandra.ethereum.model")

    override fun getKeyspaceCreations(): List<CreateKeyspaceSpecification> {
        return super.getKeyspaceCreations() + listOf(defaultKeyspaceSpecification(chainInfo().keyspace))
    }

    @Bean
    fun migrationSettings(): MigrationSettings {
        return BlockchainMigrationSettings(chainInfo())
    }

    @Bean("ethereumCassandraTemplate")
    fun reactiveCassandraTemplate(
        @Qualifier("ethereumReactiveSession") session: ReactiveSession
    ): ReactiveCassandraOperations {
        return ReactiveCassandraTemplate(DefaultReactiveSessionFactory(session), cassandraConverter())
    }

    @Bean("ethereumReactiveSession")
    fun reactiveSession(
        @Qualifier("ethereumSession") session: CassandraSessionFactoryBean
    ): ReactiveSession {
        return DefaultBridgedReactiveSession(session.`object`)
    }

    override fun getClusterBuilderConfigurer(): ClusterBuilderConfigurer? {
        return ClusterBuilderConfigurer { clusterBuilder ->
            clusterBuilder.configuration.codecRegistry.register(InstantCodec.instance)
            return@ClusterBuilderConfigurer clusterBuilder
        }
    }

    @Bean("ethereumSession")
    override fun session(): CassandraSessionFactoryBean {
        val session = super.session()
        session.setKeyspaceName(keyspaceName)
        return session
    }

    @Bean
    override fun customConversions(): CustomConversions {
        return customEthereumConversions()
    }
}

private fun customEthereumConversions(): CassandraCustomConversions {
    val additionConverters = listOf(
        TxTraceReadConverter(jsonDeserializer), TxTraceWriteConverter(jsonSerializer)
    )
    return CassandraCustomConversions(additionConverters)
}


@Component("ethereum-search-repositories")
@Conditional(NoChainCondition::class)
class EthereumSearchRepositoriesConfiguration : SearchRepositoriesConfiguration(
    entityBasePackage = "fund.cyber.cassandra.ethereum.model", keyspacePrefix = "ethereum"
) {

    override fun repositoriesClasses(): List<Class<*>> = listOf(
        EthereumBlockRepository::class.java,
        PageableEthereumBlockTxRepository::class.java,
        EthereumTxRepository::class.java,
        EthereumContractRepository::class.java,
        EthereumContractTxRepository::class.java,
        PageableEthereumContractTxRepository::class.java,
        PageableEthereumContractMinedUncleRepository::class.java,
        PageableEthereumContractMinedBlockRepository::class.java,
        EthereumUncleRepository::class.java
    )

    override fun customConversions() = customEthereumConversions()

}
