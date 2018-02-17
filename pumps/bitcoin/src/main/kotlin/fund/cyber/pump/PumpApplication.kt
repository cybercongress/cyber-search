package fund.cyber.pump

import fund.cyber.pump.common.ChainPump
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.BitcoinFamilyChain
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan


@SpringBootApplication(exclude = [KafkaAutoConfiguration::class])
@ComponentScan(basePackageClasses = [BitcoinPumpApplication::class])
class BitcoinPumpApplication {

    @Bean
    fun chain(): BitcoinFamilyChain {
        val chainAsString = env(CHAIN, "")
        return BitcoinFamilyChain.valueOf(chainAsString)
    }
}


fun main(args: Array<String>) {

    val application = SpringApplication(BitcoinPumpApplication::class.java)
    application.setRegisterShutdownHook(false)
    val applicationContext = application.run(*args)
    val pump = applicationContext.getBean(ChainPump::class.java)
    pump.startPump()
}