package fund.cyber.contract.bitcoin.summary

import fund.cyber.contract.common.delta.ContractSummaryDelta
import fund.cyber.contract.common.delta.DeltaMerger
import fund.cyber.contract.common.delta.DeltaProcessor
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.cassandra.common.CqlContractSummary
import fund.cyber.common.sumByDecimal
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.BigDecimal.ZERO
import java.time.Instant

data class BitcoinContractSummaryDelta(
        override val contract: String,
        val balanceDelta: BigDecimal,
        val totalReceivedDelta: BigDecimal,
        val txNumberDelta: Int,
        val time: Instant,
        override val topic: String,
        override val partition: Int,
        override val offset: Long
) : ContractSummaryDelta<CqlBitcoinContractSummary> {

    fun revertedDelta(): BitcoinContractSummaryDelta = BitcoinContractSummaryDelta(
            contract = contract, balanceDelta = -balanceDelta, txNumberDelta = -txNumberDelta,
            totalReceivedDelta = -totalReceivedDelta, topic = topic,
            partition = partition, offset = offset, time = time
    )

    override fun createSummary(): CqlBitcoinContractSummary {
        return CqlBitcoinContractSummary(
                hash = this.contract, confirmedBalance = this.balanceDelta.toString(),
                confirmedTxNumber = this.txNumberDelta,
                confirmedTotalReceived = this.totalReceivedDelta.toString(),
                firstActivityDate = this.time, lastActivityDate = this.time,
                kafkaDeltaOffset = this.offset, kafkaDeltaTopic = this.topic,
                kafkaDeltaPartition = this.partition, version = 0
        )
    }

    override fun updateSummary(summary: CqlBitcoinContractSummary): CqlBitcoinContractSummary {
        return CqlBitcoinContractSummary(
                hash = summary.hash,
                confirmedBalance = (BigDecimal(summary.confirmedBalance) + this.balanceDelta).toString(),
                confirmedTxNumber = summary.confirmedTxNumber + this.txNumberDelta,
                firstActivityDate = summary.firstActivityDate, lastActivityDate = time,
                confirmedTotalReceived = (BigDecimal(summary.confirmedTotalReceived) + this.totalReceivedDelta)
                        .toString(),
                kafkaDeltaOffset = this.offset, kafkaDeltaTopic = this.topic,
                kafkaDeltaPartition = this.partition, version = summary.version + 1
        )
    }
}

//todo: txNumberDelta should be 1 if contract both in ins and outs
@Component
class BitcoinTxDeltaProcessor : DeltaProcessor<BitcoinTx, CqlBitcoinContractSummary, BitcoinContractSummaryDelta> {

    override fun recordToDeltas(record: ConsumerRecord<PumpEvent, BitcoinTx>): List<BitcoinContractSummaryDelta> {
        val tx = record.value()
        val event = record.key()

        val contractsDeltasByIns = tx.ins.flatMap { input ->
            input.contracts.map { contract ->
                BitcoinContractSummaryDelta(
                        contract = contract, balanceDelta = -input.amount, txNumberDelta = 1,
                        totalReceivedDelta = ZERO, topic = record.topic(), partition = record.partition(),
                        offset = record.offset(), time = tx.blockTime
                )
            }
        }

        val contractsDeltasByOuts = tx.outs.flatMap { output ->
            output.contracts.map { contract ->
                BitcoinContractSummaryDelta(
                        contract = contract, balanceDelta = output.amount, txNumberDelta = 1,
                        totalReceivedDelta = output.amount, topic = record.topic(), partition = record.partition(),
                        offset = record.offset(), time = tx.blockTime
                )
            }
        }

        return (contractsDeltasByIns + contractsDeltasByOuts)
                .map { delta -> if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta }
    }

    override fun affectedContracts(records: List<ConsumerRecord<PumpEvent, BitcoinTx>>): Set<String> {
        val allContracts: List<String> = records.flatMap { record ->
            val inContracts = record.value().ins.flatMap { input -> input.contracts }
            val outContracts = record.value().outs.flatMap { output -> output.contracts }
            return@flatMap inContracts + outContracts
        }

        return allContracts.toSet()
    }

}

@Component
class BitcoinDeltaMerger: DeltaMerger<BitcoinContractSummaryDelta> {

    override fun mergeDeltas(deltas: Iterable<BitcoinContractSummaryDelta>,
                             currentContracts: Map<String, CqlContractSummary>): BitcoinContractSummaryDelta? {

        val first = deltas.first()
        val existingSummary = currentContracts[first.contract]


        val deltasToApply = deltas.filterNot { delta ->
            existingSummary != null && existingSummary.kafkaDeltaTopic == delta.topic
                    && existingSummary.kafkaDeltaPartition == delta.partition
                    && delta.offset <= existingSummary.kafkaDeltaOffset
        }
        val balance = deltasToApply.sumByDecimal { delta -> delta.balanceDelta }
        val totalReceived = deltasToApply.sumByDecimal { delta -> delta.totalReceivedDelta }
        val txNumber = deltasToApply.sumBy { delta -> delta.txNumberDelta }

        return if (deltasToApply.isEmpty()) null else BitcoinContractSummaryDelta(
                contract = first.contract, balanceDelta = balance, txNumberDelta = txNumber,
                totalReceivedDelta = totalReceived, topic = first.topic, partition = first.partition,
                offset = deltasToApply.maxBy { it -> it.offset }!!.offset,
                time = deltasToApply.sortedByDescending { it -> it.time }.first().time
        )
    }
}
