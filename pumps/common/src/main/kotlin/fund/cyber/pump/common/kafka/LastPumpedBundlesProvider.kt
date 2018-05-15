package fund.cyber.pump.common.kafka

import fund.cyber.common.kafka.kafkaTopicName
import fund.cyber.common.kafka.reader.SinglePartitionTopicLastItemsReader
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.BlockEntity
import fund.cyber.search.model.chains.ChainEntityType
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component


@Component
class LastPumpedBlockNumberProvider(
    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private val kafkaBrokers: String,
    private val chainInfo: ChainInfo
) {

    //todo return transactions
    //todo return last 20items
    fun getLastBlockNumber(): Long {
        val blockClass = chainInfo.entityClassByType(ChainEntityType.BLOCK)!!

        val blockTopicReader = SinglePartitionTopicLastItemsReader(
            kafkaBrokers = kafkaBrokers, topic = ChainEntityType.BLOCK.kafkaTopicName(chainInfo),
            keyClass = PumpEvent::class.java, valueClass = blockClass
        )

        val (event, block) = blockTopicReader
            .readLastRecords(1, { event -> event == PumpEvent.NEW_BLOCK })
            .firstOrNull() ?: return -1L

        return (block as BlockEntity).number
    }
}
