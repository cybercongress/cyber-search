package fund.cyber

import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.EthereumFamilyChain
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.context.annotation.Bean


@SpringBootApplication(exclude = [CassandraDataAutoConfiguration::class, KafkaAutoConfiguration::class])
class UpdateEthereumContractSummaryApplication {

    @Bean
    fun chain(): EthereumFamilyChain {
        val chainAsString = env(CHAIN, "")
        return EthereumFamilyChain.valueOf(chainAsString)
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication.run(UpdateEthereumContractSummaryApplication::class.java, *args)
        }
    }
}
