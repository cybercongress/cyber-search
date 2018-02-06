package fund.cyber.pump

import fund.cyber.pump.common.ChainPump
import org.springframework.boot.SpringApplication


object Application {

    @JvmStatic
    fun main(args: Array<String>) {

        val application = SpringApplication(Application::class.java)
        application.setRegisterShutdownHook(false)

        val applicationContext = application.run(*args)
        val pump = applicationContext.getBean(ChainPump::class.java)
        pump.startPump()
    }
}