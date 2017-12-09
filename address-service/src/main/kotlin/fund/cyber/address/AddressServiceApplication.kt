package fund.cyber.address

import fund.cyber.address.bitcoin.BitcoinAddressStateUpdatesEmitingProcess
import fund.cyber.address.bitcoin.BitcoinAddressUpdatesPersistenceProcess
import fund.cyber.cassandra.CassandraService
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.common.Chain.BITCOIN_CASH
import java.util.concurrent.Executors


object AddressServiceApplication {

    @JvmStatic
    fun main(args: Array<String>) {

        val cassandraService = CassandraService(ServiceConfiguration.cassandraServers, ServiceConfiguration.cassandraPort)

        val bitcoinAddressUpdatesPersistenceProcess = BitcoinAddressUpdatesPersistenceProcess(
                chain = BITCOIN, repository = cassandraService.bitcoinRepository
        )

        val bitcoinCashAddressUpdatesPersistenceProcess = BitcoinAddressUpdatesPersistenceProcess(
                chain = BITCOIN_CASH, repository = cassandraService.bitcoinCashRepository
        )

        val bitcoinAddressStateUpdatesEmitingProcess = BitcoinAddressStateUpdatesEmitingProcess(BITCOIN)
        val bitcoinCashAddressStateUpdatesEmitingProcess = BitcoinAddressStateUpdatesEmitingProcess(BITCOIN_CASH)

        val consumers = listOf(

                bitcoinAddressUpdatesPersistenceProcess,
                bitcoinAddressStateUpdatesEmitingProcess,

                bitcoinCashAddressUpdatesPersistenceProcess,
                bitcoinCashAddressStateUpdatesEmitingProcess
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