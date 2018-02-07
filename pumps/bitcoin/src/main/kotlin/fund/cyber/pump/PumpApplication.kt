package fund.cyber.pump

import fund.cyber.pump.common.ChainPump
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.BitcoinFamilyChain
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean


@SpringBootApplication
open class BitcoinPumpApplication {

    @Bean
    open fun chain(): BitcoinFamilyChain {
        val chainAsString = env(CHAIN, "")
        return BitcoinFamilyChain.valueOf(chainAsString)
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {

            val application = SpringApplication(BitcoinPumpApplication::class.java)
            application.setRegisterShutdownHook(false)

            val applicationContext = application.run(*args)
            val pump = applicationContext.getBean(ChainPump::class.java)
            pump.startPump()
        }
    }
}