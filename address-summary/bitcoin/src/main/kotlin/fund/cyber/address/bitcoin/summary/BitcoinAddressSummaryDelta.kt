package fund.cyber.address.bitcoin.summary

import fund.cyber.address.common.delta.AddressSummaryDelta
import fund.cyber.address.common.delta.DeltaMerger
import fund.cyber.address.common.delta.DeltaProcessor
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressSummary
import fund.cyber.cassandra.common.CqlAddressSummary
import fund.cyber.common.sumByDecimal
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.BigDecimal.ZERO
import java.time.Instant

data class BitcoinAddressSummaryDelta(
        override val address: String,
        val balanceDelta: BigDecimal,
        val totalReceivedDelta: BigDecimal,
        val txNumberDelta: Int,
        val time: Instant,
        override val topic: String,
        override val partition: Int,
        override val offset: Long
) : AddressSummaryDelta<CqlBitcoinAddressSummary> {

    fun revertedDelta(): BitcoinAddressSummaryDelta = BitcoinAddressSummaryDelta(
            address = address, balanceDelta = -balanceDelta, txNumberDelta = -txNumberDelta,
            totalReceivedDelta = -totalReceivedDelta, topic = topic,
            partition = partition, offset = offset, time = time
    )

    override fun createSummary(): CqlBitcoinAddressSummary {
        return CqlBitcoinAddressSummary(
                id = this.address, confirmedBalance = this.balanceDelta,
                confirmedTxNumber = this.txNumberDelta,
                confirmedTotalReceived = this.totalReceivedDelta,
                firstActivityDate = this.time, lastActivityDate = this.time,
                kafkaDeltaOffset = this.offset, kafkaDeltaTopic = this.topic,
                kafkaDeltaPartition = this.partition, version = 0
        )
    }

    override fun updateSummary(summary: CqlBitcoinAddressSummary): CqlBitcoinAddressSummary {
        return CqlBitcoinAddressSummary(
                id = summary.id, confirmedBalance = summary.confirmedBalance + this.balanceDelta,
                confirmedTxNumber = summary.confirmedTxNumber + this.txNumberDelta,
                firstActivityDate = summary.firstActivityDate, lastActivityDate = time,
                confirmedTotalReceived = summary.confirmedTotalReceived + this.totalReceivedDelta,
                kafkaDeltaOffset = this.offset, kafkaDeltaTopic = this.topic,
                kafkaDeltaPartition = this.partition, version = summary.version + 1
        )
    }
}

//todo: txNumberDelta should be 1 if address both in ins and outs
@Component
class BitcoinTxDeltaProcessor : DeltaProcessor<BitcoinTx, CqlBitcoinAddressSummary, BitcoinAddressSummaryDelta> {

    override fun recordToDeltas(record: ConsumerRecord<PumpEvent, BitcoinTx>): List<BitcoinAddressSummaryDelta> {
        val tx = record.value()
        val event = record.key()

        val addressesDeltasByIns = tx.ins.flatMap { input ->
            input.addresses.map { address ->
                BitcoinAddressSummaryDelta(
                        address = address, balanceDelta = -input.amount, txNumberDelta = 1,
                        totalReceivedDelta = ZERO, topic = record.topic(), partition = record.partition(),
                        offset = record.offset(), time = tx.blockTime
                )
            }
        }

        val addressesDeltasByOuts = tx.outs.flatMap { output ->
            output.addresses.map { address ->
                BitcoinAddressSummaryDelta(
                        address = address, balanceDelta = output.amount, txNumberDelta = 1,
                        totalReceivedDelta = output.amount, topic = record.topic(), partition = record.partition(),
                        offset = record.offset(), time = tx.blockTime
                )
            }
        }

        return (addressesDeltasByIns + addressesDeltasByOuts)
                .map { delta -> if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta }
    }

    override fun affectedAddresses(records: List<ConsumerRecord<PumpEvent, BitcoinTx>>): Set<String> {
        val allAddresses: List<String> = records.flatMap { record ->
            val inAddresses = record.value().ins.flatMap { input -> input.addresses }
            val outAddresses = record.value().outs.flatMap { output -> output.addresses }
            return@flatMap inAddresses + outAddresses
        }

        return allAddresses.toSet()
    }

}

@Component
class BitcoinDeltaMerger: DeltaMerger<BitcoinAddressSummaryDelta> {

    override fun mergeDeltas(deltas: Iterable<BitcoinAddressSummaryDelta>,
                             currentAddresses: Map<String, CqlAddressSummary>): BitcoinAddressSummaryDelta? {

        val first = deltas.first()
        val existingSummary = currentAddresses[first.address]


        val deltasToApply = deltas.filterNot { delta ->
            existingSummary != null && existingSummary.kafkaDeltaTopic == delta.topic
                    && existingSummary.kafkaDeltaPartition == delta.partition
                    && delta.offset <= existingSummary.kafkaDeltaOffset
        }
        val balance = deltasToApply.sumByDecimal { delta -> delta.balanceDelta }
        val totalReceived = deltasToApply.sumByDecimal { delta -> delta.totalReceivedDelta }
        val txNumber = deltasToApply.sumBy { delta -> delta.txNumberDelta }

        return if (deltasToApply.isEmpty()) null else BitcoinAddressSummaryDelta(
                address = first.address, balanceDelta = balance, txNumberDelta = txNumber,
                totalReceivedDelta = totalReceived, topic = first.topic, partition = first.partition,
                offset = deltasToApply.maxBy { it -> it.offset }!!.offset,
                time = deltasToApply.sortedByDescending { it -> it.time }.first().time
        )
    }
}
