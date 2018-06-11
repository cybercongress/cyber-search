package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractUncleRepository
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
import fund.cyber.dump.common.MessageListenerResolver
import fund.cyber.search.configuration.REALTIME_INDEXATION_TRESHOLD
import fund.cyber.search.configuration.REALTIME_INDEXATION_TRESHOLD_DEFAULT
import fund.cyber.search.model.chains.ChainEntityType
import fund.cyber.search.model.chains.ChainEntityType.BLOCK
import fund.cyber.search.model.chains.ChainEntityType.TX
import fund.cyber.search.model.chains.ChainEntityType.UNCLE
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.listener.BatchMessageListener
import org.springframework.stereotype.Component


@Component
class EthereumMessageListenerResolver(
    private val chain: ChainInfo,
    @Value("\${$REALTIME_INDEXATION_TRESHOLD:$REALTIME_INDEXATION_TRESHOLD_DEFAULT}")
    private val realtimeIndexationThreshold: Long,
    private val blockRepository: EthereumBlockRepository,
    private val contractMinedBlockRepository: EthereumContractMinedBlockRepository,
    private val txRepository: EthereumTxRepository,
    private val blockTxRepository: EthereumBlockTxRepository,
    private val contractTxRepository: EthereumContractTxRepository,
    private val uncleRepository: EthereumUncleRepository,
    private val contractUncleRepository: EthereumContractUncleRepository
) : MessageListenerResolver {

    override fun getListenerByType(chainEntityType: ChainEntityType): BatchMessageListener<PumpEvent, *> {
        return when (chainEntityType) {
            BLOCK -> BlockDumpProcess(blockRepository, contractMinedBlockRepository, chain)
            TX -> TxDumpProcess(txRepository, blockTxRepository, contractTxRepository, chain,
                realtimeIndexationThreshold)
            UNCLE -> UncleDumpProcess(uncleRepository, contractUncleRepository, chain)
        }
    }

}
