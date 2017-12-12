package fund.cyber.address

import fund.cyber.address.bitcoin.getBitcoinAddressesUpdateProcessParameters
import fund.cyber.address.common.AddressesUpdateProcessParameters
import fund.cyber.address.common.ApplyingAddressDeltasProcess
import fund.cyber.address.common.ConvertEntityToAddressDeltaProcess
import fund.cyber.address.ethereum.getEthereumAddressesUpdateProcessParameters
import fund.cyber.cassandra.CassandraService
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.cassandra.repository.EthereumKeyspaceRepository
import fund.cyber.node.common.Chain.*
import fund.cyber.node.kafka.KafkaConsumerRunner
import fund.cyber.node.kafka.KafkaEvent
import org.ehcache.CacheManager
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.xml.XmlConfiguration
import java.util.concurrent.Executors


object AddressServiceApplication {

    @JvmStatic
    fun main(args: Array<String>) {

        try {
            run()
        } catch (e: Exception) {
            Runtime.getRuntime().exit(-1)
        }
    }

    private fun run() {

        val cassandraService = CassandraService(ServiceConfiguration.cassandraServers, ServiceConfiguration.cassandraPort)
        val cacheManager = getCacheManager()

        val bitcoinProcesses = listOf(BITCOIN, BITCOIN_CASH).map { chain ->
            val repository = cassandraService.getChainRepository(chain) as BitcoinKeyspaceRepository
            val parameters = getBitcoinAddressesUpdateProcessParameters(chain, repository, cacheManager)
            return@map toUpdateAddressesStateProcess(parameters)
        }.flatten()

        val ethereumProcesses = listOf(ETHEREUM, ETHEREUM_CLASSIC).map { chain ->
            val repository = cassandraService.getChainRepository(chain) as EthereumKeyspaceRepository
            val parameters = getEthereumAddressesUpdateProcessParameters(chain, repository, cacheManager)
            return@map toUpdateAddressesStateProcess(parameters)
        }.flatten()

        val processes = bitcoinProcesses + ethereumProcesses

        val executor = Executors.newFixedThreadPool(processes.size * 2)
        processes.forEach { process -> executor.execute(process) }

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                for (process in processes) {
                    process.shutdown()
                }
                executor.shutdown()
            }
        })
    }

    private fun toUpdateAddressesStateProcess(processParameters: AddressesUpdateProcessParameters)
            : List<KafkaConsumerRunner<KafkaEvent, out Any>> {

        val chain = processParameters.chain

        val deltasEmitingProcesses = processParameters.convertEntityToAddressDeltaProcessesParameters
                .map { parameters -> ConvertEntityToAddressDeltaProcess(chain, parameters) }

        val applyDeltasProcess = ApplyingAddressDeltasProcess(chain, processParameters.applyAddressDeltaFunction)

        return deltasEmitingProcesses + applyDeltasProcess
    }

    //todo temp solution, move cache to cassandra service layer
    fun getCacheManager(): CacheManager {
        val ehcacheSettingsUri = AddressServiceApplication::class.java.getResource("/ehcache.xml")
        val cacheManager = CacheManagerBuilder.newCacheManager(XmlConfiguration(ehcacheSettingsUri))
        cacheManager.init()
        return cacheManager
    }
}