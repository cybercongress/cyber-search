package cyber.search.bitcoin.configuration

import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.data.cassandra.config.AbstractReactiveCassandraConfiguration
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories



@EnableReactiveCassandraRepositories(basePackages = ["cyber.search.bitcoin.repository"])
abstract class BaseBitcoinRepositoriesConfiguration : AbstractReactiveCassandraConfiguration() {

    override fun getEntityBasePackages(): Array<String> = arrayOf("cyber.search.bitcoin.model")
}

@Profile("bitcoin")
@Configuration
open class BitcoinRepositoryConfiguration : BaseBitcoinRepositoriesConfiguration() {

    override fun getKeyspaceName(): String = "bitcoin"
}


@Profile("bitcoin_cash")
@Configuration
open class BitcoinCaseRepositoryConfiguration : BaseBitcoinRepositoriesConfiguration() {

    override fun getKeyspaceName(): String = "bitcoin_cash"
}