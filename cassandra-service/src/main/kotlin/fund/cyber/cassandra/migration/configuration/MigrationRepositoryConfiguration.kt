package fund.cyber.cassandra.migration.configuration

import fund.cyber.cassandra.configuration.CassandraRepositoriesConfiguration
import fund.cyber.search.configuration.*
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.*
import org.springframework.data.cassandra.ReactiveSession
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean
import org.springframework.data.cassandra.core.ReactiveCassandraOperations
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories


@Configuration
@EnableReactiveCassandraRepositories(
        basePackages = ["fund.cyber.cassandra.migration.repository"],
        reactiveCassandraTemplateRef = "keyspaceMigrationCassandraTemplate"
)
class MigrationRepositoryConfiguration(
        @Value("\${$CASSANDRA_HOSTS:$CASSANDRA_HOSTS_DEFAULT}")
        private val cassandraHosts: String,
        @Value("\${$CASSANDRA_PORT:$CASSANDRA_PORT_DEFAULT}")
        private val cassandraPort: Int
) : CassandraRepositoriesConfiguration(cassandraHosts, cassandraPort) {


    override fun getKeyspaceName(): String = "cyber_system"
    override fun getEntityBasePackages(): Array<String> = arrayOf("fund.cyber.cassandra.migration.model")

    @Bean("keyspaceMigrationCassandraTemplate")
    fun reactiveCassandraTemplate(
            @Qualifier("keyspaceReactiveMigrationSession") session: ReactiveSession
    ): ReactiveCassandraOperations {
        return ReactiveCassandraTemplate(DefaultReactiveSessionFactory(session), cassandraConverter())
    }

    @Bean("keyspaceReactiveMigrationSession")
    fun reactiveSession(
            @Qualifier("keyspaceMigrationSession") session: CassandraSessionFactoryBean
    ): ReactiveSession {
        return DefaultBridgedReactiveSession(session.`object`)
    }


    @Bean("keyspaceMigrationSession")
    override fun session(): CassandraSessionFactoryBean {
        val session = super.session()
        session.setKeyspaceName(keyspaceName)
        return session
    }
}

