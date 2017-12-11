package fund.cyber.address

import fund.cyber.address.bitcoin.getBitcoinAddressesUpdateProcessParameters
import fund.cyber.address.common.AddressesUpdateProcessParameters
import fund.cyber.address.common.ApplyingAddressDeltasProcess
import fund.cyber.address.common.ConvertEntityToAddressDeltaProcess
import fund.cyber.address.ethereum.getEthereumAddressesUpdateProcessParameters
import fund.cyber.cassandra.CassandraService
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.cassandra.repository.EthereumKeyspaceRepository
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.common.Chain.ETHEREUM
import fund.cyber.node.kafka.KafkaConsumerRunner
import fund.cyber.node.kafka.KafkaEvent
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

        val bitcoinProcesses = listOf(BITCOIN).map { chain ->
            val repository = cassandraService.getChainRepository(chain) as BitcoinKeyspaceRepository
            val parameters = getBitcoinAddressesUpdateProcessParameters(chain, repository)
            return@map toUpdateAddressesStateProcess(parameters)
        }.flatten()

        val ethereumProcesses = listOf(ETHEREUM).map { chain ->
            val repository = cassandraService.getChainRepository(chain) as EthereumKeyspaceRepository
            val parameters = getEthereumAddressesUpdateProcessParameters(chain, repository)
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
}