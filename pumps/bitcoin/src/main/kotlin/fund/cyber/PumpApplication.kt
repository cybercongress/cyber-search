package fund.cyber

import fund.cyber.pump.common.runPump
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration


@SpringBootApplication(exclude = [KafkaAutoConfiguration::class])
class BitcoinPumpApplication {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {

            val application = SpringApplication(BitcoinPumpApplication::class.java)
            application.runPump(args)
        }
    }
}
