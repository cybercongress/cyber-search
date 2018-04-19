package fund.cyber.contract.common.delta

import fund.cyber.cassandra.common.CqlContractSummary
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord

interface ContractSummaryDelta <S: CqlContractSummary> {
    val contract: String
    val topic: String
    val partition: Int
    val offset: Long

    fun createSummary(): S
    fun updateSummary(summary: S): S
}

//todo this class should not be aware of kafka records
interface DeltaProcessor<R, S: CqlContractSummary, out D: ContractSummaryDelta<S>> {
    fun recordToDeltas(record: ConsumerRecord<PumpEvent, R>): List<D>
    fun affectedContracts(records: List<ConsumerRecord<PumpEvent, R>>): Set<String>
}

//todo this class should not be aware of kafka records
interface DeltaMerger<D: ContractSummaryDelta<*>> {
    fun mergeDeltas(deltas: Iterable<D>, currentContracts: Map<String, CqlContractSummary>): D?
}
