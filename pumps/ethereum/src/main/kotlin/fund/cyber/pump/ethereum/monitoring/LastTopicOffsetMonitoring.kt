package fund.cyber.pump.ethereum.monitoring

import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import fund.cyber.search.model.events.unclePumpTopic
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

@Component
class LastTopicOffsetMonitoring(
        private val monitoring: MeterRegistry,
        private val consumer: Consumer<Any, Any>,
        private val chain: EthereumFamilyChain
) {

    private val lastTxTopicOffsetMonitor = monitoring.gauge("pump_topic_last_offset",
            Tags.of("topic", chain.txPumpTopic), AtomicLong(lastOffset(chain.txPumpTopic)))!!

    private val lastBlockTopicOffsetMonitor = monitoring.gauge("pump_topic_last_offset",
            Tags.of("topic", chain.blockPumpTopic), AtomicLong(lastOffset(chain.blockPumpTopic)))!!

    private val lastUncleTopicOffsetMonitor = monitoring.gauge("pump_topic_last_offset",
            Tags.of("topic", chain.unclePumpTopic), AtomicLong(lastOffset(chain.unclePumpTopic)))!!

    @Scheduled(fixedRate = 10 * 1000)
    fun getLastNetworkBlockNumber() {
        try {
            val lastTxTopicOffset = lastOffset(chain.txPumpTopic)
            lastTxTopicOffsetMonitor.set(lastTxTopicOffset)

            val lastBlockTopicOffset = lastOffset(chain.blockPumpTopic)
            lastBlockTopicOffsetMonitor.set(lastBlockTopicOffset)

            val lastUncleTopicOffset = lastOffset(chain.unclePumpTopic)
            lastUncleTopicOffsetMonitor.set(lastUncleTopicOffset)
        } catch (e: Exception) {
        }
    }

    private final fun lastOffset(topic: String): Long {
        val partitions = consumer.partitionsFor(topic)
        if (partitions == null || partitions.size == 0) return -1L
        if (partitions.size > 1) throw RuntimeException("Topic have more than one partition")

        val partition = TopicPartition(partitions.first().topic(), partitions.first().partition())
        consumer.assign(listOf(partition))
        consumer.seekToEnd(listOf(partition))
        return consumer.position(partition) - 1
    }

}