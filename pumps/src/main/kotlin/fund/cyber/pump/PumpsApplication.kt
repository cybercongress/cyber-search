package fund.cyber.pump

import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.*
import fund.cyber.pump.bitcoin.BitcoinBlockchainInterface
import fund.cyber.pump.bitcoin.BitcoinCassandraActionFactory
import fund.cyber.pump.bitcoin_cash.BitcoinCashBlockchainInterface
import fund.cyber.pump.ethereum.EthereumBlockchainInterface
import fund.cyber.pump.ethereum.EthereumCassandraActionFactory
import fund.cyber.pump.ethereum_classic.EthereumClassicBlockchainInterface


object PumpsApplication {

    private val storages: List<StorageInterface> = listOf(PumpsContext.elassandraStorage)

    @JvmStatic
    fun main(args: Array<String>) {
        PumpsConfiguration.chainsToPump.forEach { chain -> startChainPumper(chain) }
    }


    private fun startChainPumper(chain: Chain) {
        when (chain) {
            BITCOIN -> {
                val flowableInterface = ConcurrentPulledBlockchain(BitcoinBlockchainInterface())
                getChainPumper(flowableInterface, listOf(BitcoinCassandraActionFactory())).start()
            }
            BITCOIN_CASH -> {
                val flowableInterface = ConcurrentPulledBlockchain(BitcoinCashBlockchainInterface())
                getChainPumper(flowableInterface, listOf(BitcoinCassandraActionFactory())).start()
            }
            ETHEREUM -> {
                val flowableInterface = ConcurrentPulledBlockchain(EthereumBlockchainInterface())
                getChainPumper(flowableInterface, listOf(EthereumCassandraActionFactory())).start()
            }
            ETHEREUM_CLASSIC -> {
                val flowableInterface = ConcurrentPulledBlockchain(EthereumClassicBlockchainInterface())
                getChainPumper(flowableInterface, listOf(EthereumCassandraActionFactory())).start()
            }
        }
    }


    private fun <T : BlockBundle> getChainPumper(
            flowableInterface: FlowableBlockchainInterface<T>, actionFactories: List<StorageActionFactory>
    ): ChainPumper<T> {

        return ChainPumper(
                blockchainInterface = flowableInterface, storageActionsFactories = actionFactories,
                storages = storages, stateStorage = PumpsContext.elassandraStorage
        )
    }
}
