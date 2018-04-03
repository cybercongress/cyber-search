package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressMinedUncle
import fund.cyber.cassandra.ethereum.model.CqlEthereumUncle
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
import fund.cyber.cassandra.ethereum.repository.EthereumAddressUncleRepository
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.unclePumpTopic
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener
import java.util.concurrent.atomic.AtomicLong


class UncleDumpProcess(
        private val uncleRepository: EthereumUncleRepository,
        private val addressUncleRepository: EthereumAddressUncleRepository,
        private val chain: EthereumFamilyChain,
        private val monitoring: MeterRegistry
) : BatchMessageListener<PumpEvent, EthereumUncle> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    private lateinit var topicCurrentOffsetMonitor: AtomicLong


    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumUncle>>) {

        log.info("Dumping batch of ${records.size} $chain uncles from offset ${records.first().offset()}")

        val recordsToProcess = records.toRecordEventsMap()
                .filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK))

        val unclesToCommit = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.NEW_BLOCK) }.keys
        val unclesToRevert = recordsToProcess.filter { entry -> entry.value.contains(PumpEvent.DROPPED_BLOCK) }.keys

        uncleRepository.saveAll(unclesToCommit.map { uncle -> CqlEthereumUncle(uncle) }).collectList().block()
        uncleRepository.deleteAll(unclesToRevert.map { uncle -> CqlEthereumUncle(uncle) }).block()

        addressUncleRepository
                .saveAll(unclesToCommit.map { uncle -> CqlEthereumAddressMinedUncle(uncle) })
                .collectList().block()
        addressUncleRepository
                .saveAll(unclesToRevert.map { uncle -> CqlEthereumAddressMinedUncle(uncle) })
                .collectList().block()

        if (::topicCurrentOffsetMonitor.isInitialized) {
            topicCurrentOffsetMonitor.set(records.last().offset())
        } else {
            topicCurrentOffsetMonitor = monitoring.gauge("dump_topic_current_offset",
                    Tags.of("topic", chain.unclePumpTopic), AtomicLong(records.last().offset()))!!
        }

    }
}
