package fund.cyber.pump

import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.*
import fund.cyber.pump.bitcoin.BitcoinBlockchainInterface
import fund.cyber.pump.bitcoin.BitcoinKafkaStorageActionTemplateFactory
import fund.cyber.pump.bitcoin_cash.BitcoinCashBlockchainInterface
import fund.cyber.pump.cassandra.SimpleCassandraActionSourceFactory
import fund.cyber.pump.ethereum.EthereumBlockchainInterface
import fund.cyber.pump.ethereum.EthereumKafkaStorageActionTemplateFactory
import fund.cyber.pump.ethereum_classic.EthereumClassicBlockchainInterface
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger(PumpsApplication::class.java)!!

object PumpsApplication {

    private val storages: List<StorageInterface> = listOf(
            PumpsContext.elassandraStorage, PumpsContext.kafkaStorage
    )
    private val stateStorage = PumpsContext.elassandraStorage

    @JvmStatic
    fun main(args: Array<String>) {
        try {
            PumpsConfiguration.chainsToPump.forEach { chain ->
                val blockchainInterface = this.blockchainInterfaceFor(chain)

                this.storages.forEach { storage ->
                    storage.initialize(blockchainInterface)

                    this.actionTemplateFactoriesFor(chain).forEach { actionTemplateFactory ->
                        storage.setStorageActionSourceFactoryFor(chain, actionTemplateFactory)
                    }
                }

                chainPumpFor(blockchainInterface, this.storages).start()
            }
        } catch (e: Exception) {
            log.error("Error during start pump application", e)
            PumpsContext.closeContext()
        }
    }

    private fun chainPumpFor(blockchainInterface: BlockchainInterface<*>, storages: List<StorageInterface>) =
            EventDrivenChainPump(
                    blockchainInterface,
                    storages,
                    PumpsContext.elassandraStorage
            )

    private fun actionTemplateFactoriesFor(chain: Chain): List<StorageActionSourceFactory> {
        return when (chain) {
            BITCOIN -> {
                listOf(
                        SimpleCassandraActionSourceFactory(),
                        BitcoinKafkaStorageActionTemplateFactory(chain)
                )
            }
            BITCOIN_CASH -> {
                listOf(
                        SimpleCassandraActionSourceFactory(),
                        BitcoinKafkaStorageActionTemplateFactory(chain)
                )
            }
            ETHEREUM -> {
                listOf(
                        SimpleCassandraActionSourceFactory(),
                        EthereumKafkaStorageActionTemplateFactory(chain)
                )
            }
            ETHEREUM_CLASSIC -> {
                listOf(
                        SimpleCassandraActionSourceFactory(),
                        EthereumKafkaStorageActionTemplateFactory(chain)
                )
            }
        }
    }

    private fun blockchainInterfaceFor(chain: Chain): BlockchainInterface<*> {
        return when (chain) {
            BITCOIN -> {
                BitcoinBlockchainInterface()
            }
            BITCOIN_CASH -> {
                BitcoinCashBlockchainInterface()
            }
            ETHEREUM -> {
                EthereumBlockchainInterface()
            }
            ETHEREUM_CLASSIC -> {
                EthereumClassicBlockchainInterface()
            }
        }
    }


    private fun <T : BlockBundle> getChainPumper(
            flowableInterface: FlowableBlockchainInterface<T>, actionFactories: List<StorageActionSourceFactory>
    ): ChainPump<T> {

        return ChainPump(
                blockchainInterface = flowableInterface, storageActionsFactories = actionFactories,
                storages = this.storages, stateStorage = this.stateStorage
        )
    }
}
