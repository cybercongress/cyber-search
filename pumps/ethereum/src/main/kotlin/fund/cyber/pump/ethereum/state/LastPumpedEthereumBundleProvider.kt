package fund.cyber.pump.ethereum.state

import fund.cyber.common.kafka.reader.SinglePartitionTopicLastItemsReader
import fund.cyber.pump.common.LastPumpedBundlesProvider
import fund.cyber.pump.common.UNKNOWN_PARENT_HASH
import fund.cyber.pump.ethereum.client.EthereumBlockBundle
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.Chain
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class LastPumpedEthereumBundleProvider(
        @Value("#{systemProperties['$KAFKA_BROKERS'] ?: '$KAFKA_BROKERS_DEFAULT'}")
        private val kafkaBrokers: String,
        private val chain: Chain
) : LastPumpedBundlesProvider<EthereumBlockBundle> {

    override fun getLastBlockBundles(): List<Pair<PumpEvent, EthereumBlockBundle>> {

        val blockTopicReader = SinglePartitionTopicLastItemsReader(
                kafkaBrokers = kafkaBrokers, topic = chain.blockPumpTopic,
                keyClass = PumpEvent::class.java, valueClass = EthereumBlock::class.java
        )
        val (event, block) = blockTopicReader.readLastRecords(1).firstOrNull() ?: return emptyList()

        val bundle = EthereumBlockBundle(
                number = block.number, hash = block.hash, parentHash = UNKNOWN_PARENT_HASH,
                block = block, transactions = emptyList(), uncles = emptyList()
        )

        return listOf(event to bundle)
    }
}