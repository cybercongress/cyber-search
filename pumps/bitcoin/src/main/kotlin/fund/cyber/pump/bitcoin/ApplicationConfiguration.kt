package fund.cyber.pump.bitcoin

import cyber.search.configuration.CHAIN
import cyber.search.configuration.env
import cyber.search.model.chains.BitcoinFamilyChain
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
open class ApplicationConfiguration {

    @Bean
    open fun chain(): BitcoinFamilyChain {
        val chainAsString = env(CHAIN, "")
        return BitcoinFamilyChain.valueOf(chainAsString)
    }
}