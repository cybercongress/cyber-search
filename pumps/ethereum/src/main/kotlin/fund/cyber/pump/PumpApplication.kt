package fund.cyber.pump

import fund.cyber.pump.common.ChainPump
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.EthereumFamilyChain
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean


@SpringBootApplication
open class EthereumPumpApplication {

    @Bean
    open fun chain(): EthereumFamilyChain {
        val chainAsString = env(CHAIN, "")
        return EthereumFamilyChain.valueOf(chainAsString)
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {

            val application = SpringApplication(EthereumPumpApplication::class.java)
            application.setRegisterShutdownHook(false)

            val applicationContext = application.run(*args)
            val pump = applicationContext.getBean(ChainPump::class.java)
            pump.startPump()
        }
    }
}