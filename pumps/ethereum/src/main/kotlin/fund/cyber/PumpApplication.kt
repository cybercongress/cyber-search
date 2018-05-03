package fund.cyber

import fund.cyber.pump.common.ChainPump
import fund.cyber.pump.common.pool.PoolPump
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.WITH_MEMPOOL
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.EthereumFamilyChain
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.context.annotation.Bean

private val log = LoggerFactory.getLogger(EthereumPumpApplication::class.java)!!

@SpringBootApplication(exclude = [KafkaAutoConfiguration::class])
class EthereumPumpApplication {

    @Bean
    fun chain(): EthereumFamilyChain {
        val chainAsString = env(CHAIN, "")
        return EthereumFamilyChain.valueOf(chainAsString)
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {

            val application = SpringApplication(EthereumPumpApplication::class.java)
            application.setRegisterShutdownHook(false)

            val applicationContext = application.run(*args)

            if (env(WITH_MEMPOOL, false)) {
                try {
                    val poolPump = applicationContext.getBean(PoolPump::class.java)
                    poolPump.startPump()
                } catch (e: NoSuchBeanDefinitionException) {
                    log.error("No pump for pool found!")
                }
            }

            val pump = applicationContext.getBean(ChainPump::class.java)
            pump.startPump()
        }
    }
}
