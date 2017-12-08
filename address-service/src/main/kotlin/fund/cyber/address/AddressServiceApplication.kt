package fund.cyber.address

import fund.cyber.address.bitcoin.BitcoinAddressStateUpdatesEmitingProcess
import fund.cyber.address.bitcoin.BitcoinAddressUpdatesPersistenceProcess
import fund.cyber.cassandra.CassandraService
import java.util.concurrent.Executors


object AddressServiceApplication {

    @JvmStatic
    fun main(args: Array<String>) {

        val cassandraService = CassandraService(ServiceConfiguration.cassandraServers, ServiceConfiguration.cassandraPort)

        val bitcoinAddressUpdatesPersistenceProcess = BitcoinAddressUpdatesPersistenceProcess(
                topic = "bitcoin_address_updates", repository = cassandraService.bitcoinRepository
        )

        val bitcoinAddressStateUpdatesEmitingProcess = BitcoinAddressStateUpdatesEmitingProcess(
                outputTopic = "bitcoin_address_updates", inputTopic = "bitcoin_tx",
                repository = cassandraService.bitcoinRepository
        )

        val consumers = listOf(
                bitcoinAddressUpdatesPersistenceProcess,
                bitcoinAddressStateUpdatesEmitingProcess
        )

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