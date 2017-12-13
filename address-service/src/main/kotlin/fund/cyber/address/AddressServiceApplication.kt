package fund.cyber.address

import fund.cyber.address.bitcoin.getBitcoinAddressesUpdateProcessParameters
import fund.cyber.address.common.*
import fund.cyber.address.ethereum.getEthereumAddressesUpdateProcessParameters
import fund.cyber.cassandra.CassandraService
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.cassandra.repository.EthereumKeyspaceRepository
import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.*
import fund.cyber.node.common.env
import fund.cyber.node.kafka.KafkaConsumerRunner
import fund.cyber.node.kafka.PumpEvent
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

        val chainsToPump: List<Chain> = env("CS_CHAINS_TO_PUMP", "")
                .split(",").map(String::trim).filter(String::isNotEmpty).map(Chain::valueOf)

        val cassandraService = CassandraService(ServiceConfiguration.cassandraServers, ServiceConfiguration.cassandraPort)
        val cacheManager = getCacheManager()

        val processes = chainsToPump
                .map { chain -> chainUpdateAddressesStateProcesses(chain, cassandraService, cacheManager) }
                .flatten()

        val executor = Executors.newFixedThreadPool(processes.size * 3)
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

    private fun chainUpdateAddressesStateProcesses(
            chain: Chain, cassandraService: CassandraService, cacheManager: CacheManager
    ): List<KafkaConsumerRunner<PumpEvent, out Any>> {

        return when (chain) {
            BITCOIN, BITCOIN_CASH -> {
                val repository = cassandraService.getChainRepository(chain) as BitcoinKeyspaceRepository
                val parameters = getBitcoinAddressesUpdateProcessParameters(chain, repository, cacheManager)
                toUpdateAddressesStateProcess(parameters)
            }
            ETHEREUM, ETHEREUM_CLASSIC -> {
                val repository = cassandraService.getChainRepository(chain) as EthereumKeyspaceRepository
                val parameters = getEthereumAddressesUpdateProcessParameters(chain, repository, cacheManager)
                toUpdateAddressesStateProcess(parameters)
            }
            else -> emptyList()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun toUpdateAddressesStateProcess(processParameters: AddressesUpdateProcessParameters<out AddressDelta>)
            : List<KafkaConsumerRunner<PumpEvent, out Any>> {

        val chain = processParameters.chain

        val deltasEmitingProcesses = processParameters.convertEntityToAddressDeltaProcessesParameters
                .map { parameters -> ConvertEntityToAddressDeltaProcess(chain, parameters) }

        val applyDeltasProcess = ApplyingAddressDeltasProcess(
                chain, processParameters.addressDeltaClassType as Class<AddressDelta>,
                processParameters.applyAddressDeltaFunction as ApplyAddressDeltaFunction<AddressDelta>
        )

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