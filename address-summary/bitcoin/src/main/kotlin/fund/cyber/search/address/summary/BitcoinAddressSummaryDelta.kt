package fund.cyber.search.address.summary

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressSummary
import fund.cyber.search.common.sumByDecimal
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal
import java.math.BigDecimal.ZERO

//todo: could be common?
fun Iterable<BitcoinAddressSummaryDelta>.mergeDeltas(
        currentAddresses: Map<String, CqlBitcoinAddressSummary>
): BitcoinAddressSummaryDelta? {

    val first = first()
    val existingSummary = currentAddresses[first.address]


    // todo: what if deltas to apply is empty? Case: we didn't commit offset range and dropped. Then restored with the same offset range
    val deltasToApply = filterNot { delta ->
        existingSummary != null && existingSummary.kafka_delta_topic == delta.topic
                && existingSummary.kafka_delta_partition == delta.partition && delta.offset <= existingSummary.kafka_delta_offset
    }
    val balance = deltasToApply.sumByDecimal { delta -> delta.balanceDelta }
    val totalReceived = deltasToApply.sumByDecimal { delta -> delta.totalReceivedDelta }
    val txNumber = deltasToApply.sumBy { delta -> delta.txNumberDelta }

    return if (deltasToApply.isEmpty()) null else BitcoinAddressSummaryDelta(
            address = first.address, blockNumber = first.blockNumber,
            balanceDelta = balance, txNumberDelta = txNumber,
            totalReceivedDelta = totalReceived, topic = first.topic, partition = first.partition,
            offset = deltasToApply.maxBy { it -> it.offset }!!.offset
    )
}

data class BitcoinAddressSummaryDelta(
        val address: String,
        val blockNumber: Long,
        val balanceDelta: BigDecimal,
        val totalReceivedDelta: BigDecimal,
        val txNumberDelta: Int,
        val topic: String,
        val partition: Int,
        val offset: Long
) {
    fun revertedDelta(): BitcoinAddressSummaryDelta = BitcoinAddressSummaryDelta(
            address = address, blockNumber = blockNumber,
            balanceDelta = -balanceDelta, txNumberDelta = -txNumberDelta,
            totalReceivedDelta = -totalReceivedDelta, topic = topic,
            partition = partition, offset = offset
    )
}


fun txToDeltas(record: ConsumerRecord<PumpEvent, BitcoinTx>)
        : List<BitcoinAddressSummaryDelta> {

    val tx = record.value()
    val event = record.key()

    val addressesDeltasByIns = tx.ins.flatMap { input ->
        input.addresses.map { address ->
            BitcoinAddressSummaryDelta(address = address, blockNumber = tx.blockNumber,
                    balanceDelta = -input.amount, txNumberDelta = 1, totalReceivedDelta = ZERO,
                    topic = record.topic(), partition = record.partition(), offset = record.offset())
        }
    }

    val addressesDeltasByOuts = tx.outs.flatMap { output ->
        output.addresses.map { address ->
            BitcoinAddressSummaryDelta(address = address, blockNumber = tx.blockNumber,
                    balanceDelta = output.amount, txNumberDelta = 1, totalReceivedDelta = output.amount,
                    topic = record.topic(), partition = record.partition(), offset = record.offset())
        }
    }

    return (addressesDeltasByIns + addressesDeltasByOuts)
            .map { delta -> if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta }
}


// todo: make common
fun getAffectedAddresses(records: List<ConsumerRecord<PumpEvent, BitcoinTx>>): Set<String> {

    val allAddresses: List<String> = records.flatMap { record ->
        val inAddresses = record.value().ins.flatMap { input -> input.addresses }
        val outAddresses = record.value().outs.flatMap { output -> output.addresses }
        return@flatMap inAddresses + outAddresses
    }

    return allAddresses.toSet()
}