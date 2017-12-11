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

    @JvmStatic
    fun main(args: Array<String>) {
        try {
            PumpsConfiguration.chainsToPump.forEach { chain -> startChainPumper(chain) }
        } catch (e: Exception) {
            log.error("Error during start pump application", e)
            PumpsContext.closeContext()
        }
    }

    private fun startChainPumper(chain: Chain) {
        when (chain) {
            BITCOIN -> {
                val actionTemplateFactories = listOf(
                        SimpleCassandraActionSourceFactory(),
                        BitcoinKafkaStorageActionTemplateFactory(chain)
                )
                val flowableInterface = ConcurrentPulledBlockchain(BitcoinBlockchainInterface())
                getChainPumper(flowableInterface, actionTemplateFactories).start()
            }
            BITCOIN_CASH -> {
                val actionTemplateFactories = listOf(
                        SimpleCassandraActionSourceFactory(),
                        BitcoinKafkaStorageActionTemplateFactory(chain)
                )
                val flowableInterface = ConcurrentPulledBlockchain(BitcoinCashBlockchainInterface())
                getChainPumper(flowableInterface, actionTemplateFactories).start()
            }
            ETHEREUM -> {
                val actionTemplateFactories = listOf(
                        SimpleCassandraActionSourceFactory(),
                        EthereumKafkaStorageActionTemplateFactory(chain)
                )
                val flowableInterface = ConcurrentPulledBlockchain(EthereumBlockchainInterface())
                getChainPumper(flowableInterface, actionTemplateFactories).start()
            }
            ETHEREUM_CLASSIC -> {
                val actionTemplateFactories = listOf(
                        SimpleCassandraActionSourceFactory(),
                        EthereumKafkaStorageActionTemplateFactory(chain)
                )
                val flowableInterface = ConcurrentPulledBlockchain(EthereumClassicBlockchainInterface())
                getChainPumper(flowableInterface, actionTemplateFactories).start()
            }
        }
    }


    private fun <T : BlockBundle> getChainPumper(
            flowableInterface: FlowableBlockchainInterface<T>, actionFactories: List<StorageActionSourceFactory>
    ): ChainPump<T> {

        return ChainPump(
                blockchainInterface = flowableInterface, storageActionsFactories = actionFactories,
                storages = storages, stateStorage = PumpsContext.elassandraStorage
        )
    }
}
