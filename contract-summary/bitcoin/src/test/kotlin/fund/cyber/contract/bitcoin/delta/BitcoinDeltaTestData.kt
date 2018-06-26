package fund.cyber.contract.bitcoin.delta

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.BitcoinTxIn
import fund.cyber.search.model.bitcoin.BitcoinTxOut
import fund.cyber.search.model.bitcoin.SignatureScript
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal
import java.time.Instant

private val chainInfo = ChainInfo(ChainFamily.BITCOIN)
private val fee = BigDecimal("0.1")

@SuppressWarnings("LongParameterList")
internal fun txRecord(event: PumpEvent, hash: String, from: String, to: String,
                     value: BigDecimal, offset: Long = 0) = ConsumerRecord<PumpEvent, BitcoinTx>(
    chainInfo.txPumpTopic, 0, offset, event,
    BitcoinTx(
        hash = hash, blockNumber = 0, blockTime = Instant.ofEpochMilli(100000), index = 0,
        blockHash = "afdfad", firstSeenTime = Instant.ofEpochMilli(100000), size = 100,
        fee = fee,
        ins = listOf(
            BitcoinTxIn(
                listOf(from), BigDecimal.ONE.plus(value).plus(fee), SignatureScript("0x", "0x"),
                txHash = "prev", txOut = 0
            )
        ),
        outs = listOf(
            BitcoinTxOut(listOf(to), value, "0x", 0, 1),
            BitcoinTxOut(listOf(from), BigDecimal.ONE, "0x", 0, 1)
        ),
        totalInputsAmount = BigDecimal.ONE.plus(value).plus(fee),
        totalOutputsAmount = BigDecimal.ONE.plus(value)
    )
)

@SuppressWarnings("LongParameterList")
internal fun delta(
    contract: String, balanceDelta: BigDecimal, reverted: Boolean = false, txNumberDelta: Int,
    totalReceived: BigDecimal? = null, offset: Long = 0
) = BitcoinContractSummaryDelta(
    contract = contract, time = Instant.ofEpochMilli(100000),
    balanceDelta = if (reverted) -balanceDelta else balanceDelta,
    totalReceivedDelta = totalReceived ?: calculateTotalReceived(reverted, balanceDelta),
    txNumberDelta = txNumberDelta, topic = chainInfo.txPumpTopic, partition = 0, offset = offset
)

internal fun calculateTotalReceived(reverted: Boolean, balanceDelta: BigDecimal) = (if (reverted && balanceDelta > BigDecimal.ZERO) -balanceDelta else if (balanceDelta > BigDecimal.ZERO) balanceDelta else BigDecimal.ZERO)!!

internal fun contractSummary(contract: String, offset: Long) = CqlBitcoinContractSummary(
    hash = contract, confirmedTxNumber = 1, confirmedBalance = "1", confirmedTotalReceived = "1",
    firstActivityDate = Instant.ofEpochMilli(100000), lastActivityDate = Instant.ofEpochMilli(100000),
    version = 0, kafkaDeltaOffset = offset, kafkaDeltaPartition = 0, kafkaDeltaTopic = chainInfo.txPumpTopic,
    kafkaDeltaOffsetCommitted = true
)