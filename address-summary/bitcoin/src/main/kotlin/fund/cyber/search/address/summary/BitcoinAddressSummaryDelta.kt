package fund.cyber.search.address.summary

import fund.cyber.search.common.sumByDecimal
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal
import java.math.BigDecimal.ZERO


fun Iterable<BitcoinAddressSummaryDelta>.mergeDeltas(): BitcoinAddressSummaryDelta {

    val first = first()

    val balance = sumByDecimal { delta -> delta.balanceDelta }
    val totalReceived = sumByDecimal { delta -> delta.totalReceivedDelta }
    val txNumber = sumBy { delta -> delta.txNumberDelta }

    return BitcoinAddressSummaryDelta(
            address = first.address, blockNumber = first.blockNumber,
            balanceDelta = balance, txNumberDelta = txNumber,
            totalReceivedDelta = totalReceived
    )
}

data class BitcoinAddressSummaryDelta(
        val address: String,
        val blockNumber: Long,
        val balanceDelta: BigDecimal,
        val totalReceivedDelta: BigDecimal,
        val txNumberDelta: Int
) {
    fun revertedDelta(): BitcoinAddressSummaryDelta = BitcoinAddressSummaryDelta(
            address = address, blockNumber = blockNumber,
            balanceDelta = -balanceDelta, txNumberDelta = -txNumberDelta,
            totalReceivedDelta = -totalReceivedDelta
    )
}


fun txToDeltas(record: ConsumerRecord<PumpEvent, BitcoinTx>)
        : List<BitcoinAddressSummaryDelta> {

    val tx = record.value()
    val event = record.key()

    val addressesDeltasByIns = tx.ins.flatMap { input ->
        input.addresses.map { address ->
            BitcoinAddressSummaryDelta(address = address, blockNumber = tx.blockNumber,
                    balanceDelta = -input.amount, txNumberDelta = 1, totalReceivedDelta = ZERO)
        }
    }

    val addressesDeltasByOuts = tx.outs.flatMap { output ->
        output.addresses.map { address ->
            BitcoinAddressSummaryDelta(address = address, blockNumber = tx.blockNumber,
                    balanceDelta = output.amount, txNumberDelta = 1, totalReceivedDelta = output.amount)
        }
    }

    return (addressesDeltasByIns + addressesDeltasByOuts)
            .map { delta -> if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta }
}