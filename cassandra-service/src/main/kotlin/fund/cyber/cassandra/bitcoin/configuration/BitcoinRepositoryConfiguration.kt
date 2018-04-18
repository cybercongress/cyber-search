package fund.cyber.cassandra.bitcoin.configuration

import com.datastax.driver.core.Cluster
import com.datastax.driver.extras.codecs.jdk8.InstantCodec
import fund.cyber.cassandra.bitcoin.repository.BitcoinAddressSummaryRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinAddressMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinAddressTxRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinBlockTxRepository
import fund.cyber.cassandra.common.NoChainCondition
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
import fund.cyber.search.model.chains.Chain
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Condition
import org.springframework.context.annotation.ConditionContext
import org.springframework.context.annotation.Conditional
import org.springframework.context.annotation.Configuration
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.type.AnnotatedTypeMetadata
import org.springframework.data.cassandra.ReactiveSession
import org.springframework.data.cassandra.config.CassandraEntityClassScanner
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean
import org.springframework.data.cassandra.config.ClusterBuilderConfigurer
import org.springframework.data.cassandra.config.SchemaAction
import org.springframework.data.cassandra.core.CassandraTemplate
import org.springframework.data.cassandra.core.ReactiveCassandraOperations
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories
import org.springframework.data.cassandra.repository.support.CassandraRepositoryFactory
import org.springframework.data.cassandra.repository.support.ReactiveCassandraRepositoryFactory
import org.springframework.stereotype.Component


@Configuration
@EnableReactiveCassandraRepositories(
        basePackages = ["fund.cyber.cassandra.bitcoin.repository"],
        reactiveCassandraTemplateRef = "bitcoinCassandraTemplate"
)
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


private class BitcoinFamilyChainCondition : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {

        val chain = context.environment.getProperty(CHAIN) ?: ""
        return BitcoinFamilyChain.values().map(BitcoinFamilyChain::name).contains(chain)
    }
}


@Component("bitcoin-cassandra-repositories")
@Conditional(NoChainCondition::class)
class BitcoinRepositoriesConfiguration : InitializingBean {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext
    @Autowired
    private lateinit var cluster: Cluster

    override fun afterPropertiesSet() {
        registerBitcoinRepositories()
    }

    fun registerBitcoinRepositories() {

        val beanFactory = applicationContext.beanFactory

        BitcoinFamilyChain.values().forEach { chain ->

            //create sessions
            val converter = MappingCassandraConverter(mappingContext(chain))
            val session = getKeyspaceSession(chain, converter).also { it.afterPropertiesSet() }
            val reactiveSession = DefaultReactiveSessionFactory(DefaultBridgedReactiveSession(session.`object`))

            // create cassandra operations
            val reactiveCassandraOperations = ReactiveCassandraTemplate(reactiveSession, converter)
            val cassandraOperations = CassandraTemplate(session.`object`, converter)

            // create repository factories
            val reactiveRepositoryFactory = ReactiveCassandraRepositoryFactory(reactiveCassandraOperations)
            val repositoryFactory = CassandraRepositoryFactory(cassandraOperations)

            // create repositories
            val blockRepository = reactiveRepositoryFactory.getRepository(BitcoinBlockRepository::class.java)
            val blockTxRepository = repositoryFactory.getRepository(PageableBitcoinBlockTxRepository::class.java)

            val txRepository = reactiveRepositoryFactory.getRepository(BitcoinTxRepository::class.java)

            val addressRepository = reactiveRepositoryFactory.getRepository(BitcoinAddressSummaryRepository::class.java)
            val addressTxRepository = repositoryFactory.getRepository(PageableBitcoinAddressTxRepository::class.java)
            val addressBlockRepository = repositoryFactory
                    .getRepository(PageableBitcoinAddressMinedBlockRepository::class.java)


            // register repositories
            beanFactory.registerSingleton(chain.name + "blockRepository", blockRepository)
            beanFactory.registerSingleton(chain.name + "pageableBlockTxRepository", blockTxRepository)

            beanFactory.registerSingleton(chain.name + "txRepository", txRepository)

            beanFactory.registerSingleton(chain.name + "addressRepository", addressRepository)
            beanFactory.registerSingleton(chain.name + "pageableAddressTxRepository", addressTxRepository)
            beanFactory.registerSingleton(chain.name + "pageableAddressBlockRepository", addressBlockRepository)
        }
    }

    private fun mappingContext(chain: Chain): CassandraMappingContext {

        val mappingContext = CassandraMappingContext()

        mappingContext.setInitialEntitySet(CassandraEntityClassScanner.scan("fund.cyber.cassandra.bitcoin.model"))
        mappingContext.setUserTypeResolver(SimpleUserTypeResolver(cluster, chain.keyspace))

        return mappingContext
    }


    private fun getKeyspaceSession(chain: Chain, converter: MappingCassandraConverter) = CassandraSessionFactoryBean()
            .apply {
                setCluster(cluster)
                setConverter(converter)
                setKeyspaceName(chain.keyspace)
                schemaAction = SchemaAction.NONE
            }
}
