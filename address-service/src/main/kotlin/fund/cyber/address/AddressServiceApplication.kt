package fund.cyber.address

import fund.cyber.address.bitcoin.BitcoinAddressStateUpdater
import java.util.concurrent.Executors


object AddressServiceApplication {

    @JvmStatic
    fun main(args: Array<String>) {

        val consumers = listOf(BitcoinAddressStateUpdater())

        val executor = Executors.newFixedThreadPool(consumers.size)
        consumers.forEach { consumer -> executor.execute(consumer) }

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                for (consumer in consumers) {
                    consumer.shutdown()
                }
                executor.shutdown()
            }
        })
    }
}