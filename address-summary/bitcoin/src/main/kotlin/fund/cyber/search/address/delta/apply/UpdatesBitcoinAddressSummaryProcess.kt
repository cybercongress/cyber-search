package fund.cyber.search.address.delta.apply


import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressSummary
import fund.cyber.cassandra.bitcoin.repository.BitcoinUpdateAddressSummaryRepository
import fund.cyber.search.address.summary.BitcoinAddressSummaryDelta
import fund.cyber.search.address.summary.mergeDeltas
import fund.cyber.search.address.summary.txToDeltas
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener
import org.springframework.stereotype.Component
import java.math.BigDecimal


/**
 *
 * This process should not be aware of chain reorganisation
 *
 * */
//todo add tests
//todo add deadlock catcher
@Component
class UpdatesBitcoinAddressSummaryProcess(
        private val addressSummaryRepository: BitcoinUpdateAddressSummaryRepository
) : BatchConsumerAwareMessageListener<PumpEvent, BitcoinTx> {


    override fun onMessage(records: List<ConsumerRecord<PumpEvent, BitcoinTx>>, consumer: Consumer<*, *>) {

        records.forEach { record ->
            val deltas = txToDeltas(record)
            processDeltas(record, deltas)
            consumer.commitSync()
            //cassandraCommit()
        }
    }

    /**
     * Returns deltas to be committed after kafka offset commit
     */
    private fun processDeltas(record: ConsumerRecord<PumpEvent, BitcoinTx>, deltas: List<BitcoinAddressSummaryDelta>)
            : List<BitcoinAddressSummaryDelta> {

        // merge deltas for same address for one tx
        val mergedDeltas = deltas.groupBy { delta -> delta.address }
                .mapValues { addressDeltas -> addressDeltas.value.mergeDeltas() }
                .values

        // filter deltas already applied and committed to kafka
        // find not applied deltas and apply them
        return emptyList()
    }


/*    fun processDelta(record: ConsumerRecord<PumpEvent, BitcoinTx>, delta: BitcoinAddressSummaryDelta) {


        val currentState = addressSummaryRepository.findById(delta.address).block()

        //first time address appear
        if (currentState == null) {
            val newState = newCqlBitcoinAddressSummary(record)
            saveAndCommit(newState, consumer)
            return
        }

        //processed, but not committed to kafka and cassandra record
        if (isKafkaUncommittedProcessedRecord(record, currentState)) {
            commit(currentState, consumer)
            return
        }

        if (!currentState.kafka_delta_offset_committed) {
            //processed, committed to kafka, but not to cassandra state. Also, consumer goes further.
            //might happen if consumer dies after committed to kafka and before committing to cassandra
            if (isCassandraUncommittedProcessedState(record, currentState, consumer)) {
                commit(currentState, consumer)
            }
            //should wait a bit, some one else committing new state
            throw RuntimeException("Concurrent address summary modification")
        }

        //update and save new state
        val newState = updateCqlBitcoinAddressSummary(currentState, record)
        saveAndCommit(newState, consumer)
    }*/


    private fun updateCqlBitcoinAddressSummary(
            currentState: CqlBitcoinAddressSummary,
            record: ConsumerRecord<PumpEvent, BitcoinAddressSummaryDelta>): CqlBitcoinAddressSummary {

        val delta = record.value()

        return CqlBitcoinAddressSummary(
                id = record.value().address,
                confirmed_balance = (BigDecimal(currentState.confirmed_balance) + delta.balanceDelta).toString(),
                confirmed_total_received = currentState.confirmed_total_received + delta.totalReceivedDelta,
                confirmed_tx_number = currentState.confirmed_tx_number + delta.txNumberDelta,
                kafka_delta_offset = record.offset(), kafka_delta_partition = record.partition().toShort()
        )
    }

    private fun newCqlBitcoinAddressSummary(
            record: ConsumerRecord<PumpEvent, BitcoinAddressSummaryDelta>) = CqlBitcoinAddressSummary(
            id = record.value().address, confirmed_balance = record.value().balanceDelta.toString(),
            confirmed_total_received = record.value().balanceDelta, confirmed_tx_number = 1,
            kafka_delta_offset = record.offset(), kafka_delta_partition = record.partition().toShort()
    )


    private fun isKafkaUncommittedProcessedRecord(
            record: ConsumerRecord<PumpEvent, BitcoinAddressSummaryDelta>, currentState: CqlBitcoinAddressSummary
    ): Boolean {
        return !currentState.kafka_delta_offset_committed
                && currentState.kafka_delta_offset == record.offset()
                && currentState.kafka_delta_partition == record.partition().toShort()
    }


    private fun isCassandraUncommittedProcessedState(
            record: ConsumerRecord<PumpEvent, BitcoinAddressSummaryDelta>, currentState: CqlBitcoinAddressSummary,
            consumer: Consumer<*, *>
    ): Boolean {

        val topicPartition = TopicPartition(record.topic(), currentState.kafka_delta_partition.toInt())
        val committedPartitionOffset = consumer.committed(topicPartition).offset()

        return currentState.kafka_delta_offset < committedPartitionOffset
    }
}