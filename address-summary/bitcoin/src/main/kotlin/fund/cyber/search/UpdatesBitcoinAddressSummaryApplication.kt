package fund.cyber.search

import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.BitcoinFamilyChain
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.EnableKafka


@EnableKafka
@SpringBootApplication
open class UpdatesBitcoinAddressSummaryApplication {

    @Bean
    open fun chain(): BitcoinFamilyChain {
        val chainAsString = env(CHAIN, "")
        return BitcoinFamilyChain.valueOf(chainAsString)
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication.run(UpdatesBitcoinAddressSummaryApplication::class.java, *args)
        }
    }
}

