package fund.cyber.cassandra.common

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.KeyspaceMetadata
import com.datastax.driver.core.PoolingOptions
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
import com.datastax.driver.core.policies.LoadBalancingPolicy
import com.datastax.driver.core.policies.TokenAwarePolicy
import fund.cyber.search.model.chains.ChainInfo
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.context.support.GenericApplicationContext
import org.springframework.data.cassandra.ReactiveSession
import org.springframework.data.cassandra.config.AbstractReactiveCassandraConfiguration
import org.springframework.data.cassandra.config.CassandraEntityClassScanner
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean
import org.springframework.data.cassandra.config.SchemaAction
import org.springframework.data.cassandra.core.CassandraTemplate
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver
import org.springframework.data.cassandra.repository.support.CassandraRepositoryFactory
import org.springframework.data.cassandra.repository.support.ReactiveCassandraRepositoryFactory
import org.springframework.data.repository.reactive.ReactiveCrudRepository

const val MAX_CONCURRENT_REQUESTS = 8182
const val MAX_PER_ROUTE = 16
const val MAX_TOTAL = 32

val ChainInfo.keyspace: String get() = nameLowerCase

const val REPOSITORY_NAME_DELIMITER = "__"

fun mappingContext(
    cluster: Cluster, keyspace: String, basePackage: String,
    customConversions: CassandraCustomConversions = CassandraCustomConversions(emptyList<Any>())
): CassandraMappingContext {

    val mappingContext = CassandraMappingContext()

    mappingContext.setInitialEntitySet(CassandraEntityClassScanner.scan(basePackage))
    mappingContext.setUserTypeResolver(SimpleUserTypeResolver(cluster, keyspace))
    mappingContext.setCustomConversions(customConversions)

    return mappingContext
}

fun getKeyspaceSession(cluster: Cluster, keyspace: String, converter: MappingCassandraConverter) =
    CassandraSessionFactoryBean().apply {
        setCluster(cluster)
        setConverter(converter)
        setKeyspaceName(keyspace)
        schemaAction = SchemaAction.NONE
    }

abstract class CassandraRepositoriesConfiguration(
    private val cassandraHosts: String,
    private val cassandraPort: Int,
    private val maxRequestLocal: Int = MAX_CONCURRENT_REQUESTS,
    private val maxRequestRemote: Int = MAX_CONCURRENT_REQUESTS
) : AbstractReactiveCassandraConfiguration() {

    override fun getPoolingOptions() = PoolingOptions()
        .setMaxRequestsPerConnection(HostDistance.LOCAL, maxRequestLocal)
        .setMaxRequestsPerConnection(HostDistance.REMOTE, maxRequestRemote)!!

    override fun getPort() = cassandraPort
    override fun getContactPoints() = cassandraHosts

    @Bean
    @Primary
    override fun reactiveSession(): ReactiveSession {
        return super.reactiveSession()
    }

    override fun getKeyspaceCreations(): List<CreateKeyspaceSpecification> {
        return listOf(defaultKeyspaceSpecification("cyber_system"))
    }

    override fun getLoadBalancingPolicy(): LoadBalancingPolicy? {
        return TokenAwarePolicy(
            DCAwareRoundRobinPolicy.builder().withLocalDc("WITHOUT_REPLICATION")
                .withUsedHostsPerRemoteDc(0)
                .build()
        )
    }

}

/**
 * Return bean name of search repository by specific chainName
 *
 * @param chainName chain name
 * @return search repository bean name
 */
fun Class<*>.searchRepositoryBeanName(chainName: String) = "$chainName$REPOSITORY_NAME_DELIMITER${this.name}"

abstract class SearchRepositoriesConfiguration(
    private val entityBasePackage: String,
    private val keyspacePrefix: String
) : InitializingBean {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext
    @Autowired
    private lateinit var cluster: Cluster

    abstract fun repositoriesClasses(): List<Class<*>>

    abstract fun customConversions(): CassandraCustomConversions

    override fun afterPropertiesSet() {
        registerRepositories()
    }

    private fun registerRepositories() {

        val beanFactory = applicationContext.beanFactory

        cluster.metadata.keyspaces
            .filter { keyspace -> keyspace.name.startsWith(keyspacePrefix, true) }
            .forEach { keyspace ->

                //create sessions
                val converter = mappingCassandraConverter(keyspace)
                converter.customConversions = customConversions()
                converter.afterPropertiesSet()
                val session = getKeyspaceSession(cluster, keyspace.name, converter).also { it.afterPropertiesSet() }
                val reactiveSession = DefaultReactiveSessionFactory(DefaultBridgedReactiveSession(session.`object`))

                // create cassandra operations
                val reactiveCassandraOperations = ReactiveCassandraTemplate(reactiveSession, converter)
                val cassandraOperations = CassandraTemplate(session.`object`, converter)

                // create repository factories
                val reactiveRepositoryFactory = ReactiveCassandraRepositoryFactory(reactiveCassandraOperations)
                val repositoryFactory = CassandraRepositoryFactory(cassandraOperations)

                repositoriesClasses().forEach { clazz ->
                    val repositoryBean = if (ReactiveCrudRepository::class.java.isAssignableFrom(clazz)) {
                        reactiveRepositoryFactory.getRepository(clazz)
                    } else {
                        repositoryFactory.getRepository(clazz)
                    }

                    beanFactory.registerSingleton(clazz.searchRepositoryBeanName(keyspace.name), repositoryBean)

                }

            }
    }

    private fun mappingCassandraConverter(keyspace: KeyspaceMetadata) =
        MappingCassandraConverter(mappingContext(cluster, keyspace.name, entityBasePackage, customConversions()))
}
