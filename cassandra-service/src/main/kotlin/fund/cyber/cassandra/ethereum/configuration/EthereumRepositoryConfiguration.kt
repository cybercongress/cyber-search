package fund.cyber.cassandra.ethereum.configuration

import com.datastax.driver.core.Cluster
import fund.cyber.cassandra.common.NoChainCondition
import fund.cyber.cassandra.configuration.CassandraRepositoriesConfiguration
import fund.cyber.cassandra.configuration.keyspace
import fund.cyber.cassandra.ethereum.repository.EthereumContractRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedUncleRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumBlockTxRepository
import fund.cyber.cassandra.migration.BlockchainMigrationSettings
import fund.cyber.cassandra.migration.MigrationSettings
import fund.cyber.search.configuration.CASSANDRA_HOSTS
import fund.cyber.search.configuration.CASSANDRA_HOSTS_DEFAULT
import fund.cyber.search.configuration.CASSANDRA_PORT
import fund.cyber.search.configuration.CASSANDRA_PORT_DEFAULT
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.Chain
import fund.cyber.search.model.chains.EthereumFamilyChain
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
        basePackages = ["fund.cyber.cassandra.ethereum.repository"],
        reactiveCassandraTemplateRef = "ethereumCassandraTemplate"
)
@Conditional(EthereumFamilyChainCondition::class)
class EthereumRepositoryConfiguration(
        @Value("\${$CASSANDRA_HOSTS:$CASSANDRA_HOSTS_DEFAULT}")
        private val cassandraHosts: String,
        @Value("\${$CASSANDRA_PORT:$CASSANDRA_PORT_DEFAULT}")
        private val cassandraPort: Int
) : CassandraRepositoriesConfiguration(cassandraHosts, cassandraPort) {

    private val chain = EthereumFamilyChain.valueOf(env(CHAIN, ""))

    override fun getKeyspaceName(): String = chain.keyspace
    override fun getEntityBasePackages(): Array<String> = arrayOf("fund.cyber.cassandra.ethereum.model")

    @Bean
    fun migrationSettings(): MigrationSettings {
        return BlockchainMigrationSettings(chain)
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


    @Bean("ethereumSession")
    override fun session(): CassandraSessionFactoryBean {
        val session = super.session()
        session.setKeyspaceName(keyspaceName)
        return session
    }
}


private class EthereumFamilyChainCondition : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {
        val chain = context.environment.getProperty(CHAIN) ?: ""
        return EthereumFamilyChain.values().map(EthereumFamilyChain::name).contains(chain)
    }
}


@Component("ethereum-cassandra-repositories")
@Conditional(NoChainCondition::class)
class EthereumRepositoriesConfiguration : InitializingBean {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext
    @Autowired
    private lateinit var cluster: Cluster

    override fun afterPropertiesSet() {
        registerEthereumRepositories()
    }

    fun registerEthereumRepositories() {

        val beanFactory = applicationContext.beanFactory

        EthereumFamilyChain.values().forEach { chain ->

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
            val blockRepository = reactiveRepositoryFactory.getRepository(EthereumBlockRepository::class.java)
            val blockTxRepository = repositoryFactory.getRepository(PageableEthereumBlockTxRepository::class.java)

            val txRepository = reactiveRepositoryFactory.getRepository(EthereumTxRepository::class.java)

            val contractRepository = reactiveRepositoryFactory.getRepository(EthereumContractRepository::class.java)
            val contractTxRepository = repositoryFactory.getRepository(PageableEthereumContractTxRepository::class.java)
            val contractUncleRepository = repositoryFactory
                    .getRepository(PageableEthereumContractMinedUncleRepository::class.java)
            val contractBlockRepository = repositoryFactory
                    .getRepository(PageableEthereumContractMinedBlockRepository::class.java)

            val uncleRepository = reactiveRepositoryFactory.getRepository(EthereumUncleRepository::class.java)

            // register repositories
            beanFactory.registerSingleton(chain.name + "blockRepository", blockRepository)
            beanFactory.registerSingleton(chain.name + "pageableBlockTxRepository", blockTxRepository)

            beanFactory.registerSingleton(chain.name + "txRepository", txRepository)

            beanFactory.registerSingleton(chain.name + "contractRepository", contractRepository)
            beanFactory.registerSingleton(chain.name + "pageableContractTxRepository", contractTxRepository)
            beanFactory.registerSingleton(chain.name + "pageableContractBlockRepository", contractBlockRepository)
            beanFactory.registerSingleton(chain.name + "pageableContractUncleRepository", contractUncleRepository)

            beanFactory.registerSingleton(chain.name + "uncleRepository", uncleRepository)
        }
    }

    private fun mappingContext(chain: Chain): CassandraMappingContext {

        val mappingContext = CassandraMappingContext()

        mappingContext.setInitialEntitySet(CassandraEntityClassScanner.scan("fund.cyber.cassandra.ethereum.model"))
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
