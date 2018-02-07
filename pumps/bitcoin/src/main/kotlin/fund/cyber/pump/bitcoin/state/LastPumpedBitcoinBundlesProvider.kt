package fund.cyber.pump.bitcoin.state


import fund.cyber.common.kafka.reader.SinglePartitionTopicLastItemsReader
import fund.cyber.pump.bitcoin.client.BitcoinBlockBundle
import fund.cyber.pump.common.LastPumpedBundlesProvider
import fund.cyber.pump.common.UNKNOWN_PARENT_HASH
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.chains.Chain
import fund.cyber.search.model.events.blockPumpTopic
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component


@Component
class LastPumpedBitcoinBundlesProvider(
        @Value("#{systemProperties['$KAFKA_BROKERS'] ?: '$KAFKA_BROKERS_DEFAULT'}")
        private val kafkaBrokers: String,
        private val chain: Chain
) : LastPumpedBundlesProvider<BitcoinBlockBundle> {


    //todo return transactions
    //todo return last 20items
    override fun getLastBlockBundles(): List<Pair<PumpEvent, BitcoinBlockBundle>> {

        val blockTopicReader = SinglePartitionTopicLastItemsReader(
                kafkaBrokers = kafkaBrokers, topic = chain.blockPumpTopic,
                keyClass = PumpEvent::class.java, valueClass = BitcoinBlock::class.java
        )
        val (event, block) = blockTopicReader.readLastRecords(1).firstOrNull() ?: return emptyList()

        val bundle = BitcoinBlockBundle(
                number = block.height, hash = block.hash, parentHash = UNKNOWN_PARENT_HASH,
                block = block, transactions = emptyList()
        )

        return listOf(event to bundle)
    }
}