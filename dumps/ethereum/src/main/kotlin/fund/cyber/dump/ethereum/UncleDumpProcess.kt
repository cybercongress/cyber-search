package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.*
import fund.cyber.cassandra.ethereum.repository.*
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


    //todo add retry
    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumUncle>>) {

        log.info("Dumping batch of ${records.size} $chain uncles from offset ${records.first().offset()}")

        val unclesToSave = records.filter { record -> record.key() == PumpEvent.NEW_BLOCK }
                .map { record -> record.value() }
                .map { uncle -> CqlEthereumUncle(uncle) }

        uncleRepository.saveAll(unclesToSave).collectList().block()

        val unclesByAddressToSave = records.filter { record -> record.key() == PumpEvent.NEW_BLOCK }
                .map { record -> record.value() }
                .map { uncle -> CqlEthereumAddressUncle(uncle) }

        addressUncleRepository.saveAll(unclesByAddressToSave).collectList().block()

        if (::topicCurrentOffsetMonitor.isInitialized) {
            topicCurrentOffsetMonitor.set(records.last().offset())
        } else {
            topicCurrentOffsetMonitor = monitoring.gauge("dump_topic_current_offset",
                    Tags.of("topic", chain.unclePumpTopic), AtomicLong(records.last().offset()))!!
        }

    }
}