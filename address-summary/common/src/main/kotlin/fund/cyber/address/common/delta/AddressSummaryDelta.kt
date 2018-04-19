package fund.cyber.address.common.delta

import fund.cyber.cassandra.common.CqlContractSummary
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord

interface AddressSummaryDelta <S: CqlContractSummary> {
    val address: String
    val topic: String
    val partition: Int
    val offset: Long

    fun createSummary(): S
    fun updateSummary(summary: S): S
}

//todo this class should not be aware of kafka records
interface DeltaProcessor<R, S: CqlContractSummary, out D: AddressSummaryDelta<S>> {
    fun recordToDeltas(record: ConsumerRecord<PumpEvent, R>): List<D>
    fun affectedAddresses(records: List<ConsumerRecord<PumpEvent, R>>): Set<String>
}

//todo this class should not be aware of kafka records
interface DeltaMerger<D: AddressSummaryDelta<*>> {
    fun mergeDeltas(deltas: Iterable<D>, currentAddresses: Map<String, CqlContractSummary>): D?
}
