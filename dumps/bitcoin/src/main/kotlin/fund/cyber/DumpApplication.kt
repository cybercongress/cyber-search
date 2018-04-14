package fund.cyber

import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.BitcoinFamilyChain
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.context.annotation.Bean

@SpringBootApplication(exclude = [CassandraDataAutoConfiguration::class, KafkaAutoConfiguration::class])
class BitcoinDumpApplication {

    @Bean
    fun chain(): BitcoinFamilyChain {
        val chainAsString = env(CHAIN, "")
        return BitcoinFamilyChain.valueOf(chainAsString)
    }

    companion object {

        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication(BitcoinDumpApplication::class.java).run(*args)
        }
    }
}
