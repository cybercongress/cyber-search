package fund.cyber.pump.common

import fund.cyber.pump.common.pool.PoolPump
import fund.cyber.search.configuration.WITH_MEMPOOL
import fund.cyber.search.configuration.env
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.boot.SpringApplication

private val log = LoggerFactory.getLogger(SpringApplication::class.java)!!

fun SpringApplication.runPump(args: Array<String>) {
    this.setRegisterShutdownHook(false)
    val applicationContext = this.run(*args)

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
