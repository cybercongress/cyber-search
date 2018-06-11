package fund.cyber.dump.common

import fund.cyber.search.model.chains.ChainEntityType
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import org.springframework.kafka.listener.BatchMessageListener

@FunctionalInterface
interface MessageListenerResolver {
    fun getListenerByType(chainEntityType: ChainEntityType): BatchMessageListener<PumpEvent, *>
}

class NoMessageListenerFoundException(
    chainEntityType: ChainEntityType,
    chainInfo: ChainInfo
): Exception("No message listener found for $chainEntityType entity of ${chainInfo.name} chain")