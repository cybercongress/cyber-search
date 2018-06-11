package fund.cyber.dump.bitcoin

import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.dump.common.MessageListenerResolver
import fund.cyber.dump.common.NoMessageListenerFoundException
import fund.cyber.search.configuration.REALTIME_INDEXATION_TRESHOLD
import fund.cyber.search.configuration.REALTIME_INDEXATION_TRESHOLD_DEFAULT
import fund.cyber.search.model.chains.ChainEntityType
import fund.cyber.search.model.chains.ChainEntityType.BLOCK
import fund.cyber.search.model.chains.ChainEntityType.TX
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.BatchMessageListener


@Configuration
class BitcoinMessageListenerResolver(
    private val chainInfo: ChainInfo,
    @Value("\${$REALTIME_INDEXATION_TRESHOLD:$REALTIME_INDEXATION_TRESHOLD_DEFAULT}")
    private val realtimeIndexationThreshold: Long,
    private val blockRepository: BitcoinBlockRepository,
    private val contractMinedBlockRepository: BitcoinContractMinedBlockRepository,
    private val txRepository: BitcoinTxRepository,
    private val contractTxRepository: BitcoinContractTxRepository,
    private val blockTxRepository: BitcoinBlockTxRepository,
    private val monitoring: MeterRegistry
) : MessageListenerResolver {


    override fun getListenerByType(chainEntityType: ChainEntityType): BatchMessageListener<PumpEvent, *> {
        return when (chainEntityType) {
            BLOCK -> BlockDumpProcess(blockRepository, contractMinedBlockRepository, chainInfo)
            TX -> TxDumpProcess(txRepository, contractTxRepository, blockTxRepository, chainInfo,
                realtimeIndexationThreshold, monitoring)
            else -> throw NoMessageListenerFoundException(chainEntityType, chainInfo)
        }
    }
}
