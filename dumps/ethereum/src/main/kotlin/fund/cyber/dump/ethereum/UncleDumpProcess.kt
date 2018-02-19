package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.*
import fund.cyber.cassandra.ethereum.repository.*
import fund.cyber.search.model.chains.Chain
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener


class UncleDumpProcess(
        private val uncleRepository: EthereumUncleRepository,
        private val addressUncleRepository: EthereumAddressUncleRepository,
        private val chain: Chain
) : BatchMessageListener<PumpEvent, EthereumUncle> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)


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

    }
}