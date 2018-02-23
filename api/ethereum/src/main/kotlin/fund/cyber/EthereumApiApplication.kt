package fund.cyber

import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.EthereumFamilyChain
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean


@SpringBootApplication(exclude = [CassandraDataAutoConfiguration::class])
class EthereumApiApplication {

    @Bean
    fun chain(): EthereumFamilyChain {
        val chainAsString = env(CHAIN, "")
        return EthereumFamilyChain.valueOf(chainAsString)
    }
}


fun main(args: Array<String>) {
    runApplication<EthereumApiApplication>(*args)
}