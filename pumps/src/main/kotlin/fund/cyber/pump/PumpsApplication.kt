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

    @JvmStatic
    fun main(args: Array<String>) {
        val storages: List<StorageInterface> = listOf(PumpsContext.elassandraStorage)
        PumpsConfiguration.chainsToPump.forEach { chain -> startChainPumper(chain, storages) }
    }
}


private fun startChainPumper(chain: Chain, storages: List<StorageInterface>) {
    when (chain) {
        BITCOIN -> {
            val flowableInterface = ConcurrentPulledBlockchain(BitcoinBlockchainInterface())
            ChainPumper(
                    blockchainInterface = flowableInterface,
                    storageActionsFactories = listOf(BitcoinCassandraActionFactory()),
                    storages = storages, stateStorage = PumpsContext.elassandraStorage
            ).start()
        }
        BITCOIN_CASH -> {
            val flowableInterface = ConcurrentPulledBlockchain(BitcoinCashBlockchainInterface())
            ChainPumper(
                    blockchainInterface = flowableInterface,
                    storageActionsFactories = listOf(BitcoinCassandraActionFactory()),
                    storages = storages, stateStorage = PumpsContext.elassandraStorage
            ).start()
        }
        ETHEREUM -> {
            val flowableInterface = ConcurrentPulledBlockchain(EthereumBlockchainInterface())
            ChainPumper(
                    blockchainInterface = flowableInterface,
                    storageActionsFactories = listOf(EthereumCassandraActionFactory()),
                    storages = storages, stateStorage = PumpsContext.elassandraStorage
            ).start()
        }
        ETHEREUM_CLASSIC -> {
            val flowableInterface = ConcurrentPulledBlockchain(EthereumClassicBlockchainInterface())
            ChainPumper(
                    blockchainInterface = flowableInterface,
                    storageActionsFactories = listOf(EthereumCassandraActionFactory()),
                    storages = storages, stateStorage = PumpsContext.elassandraStorage
            ).start()
        }
    }
}