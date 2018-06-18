package fund.cyber.api.bitcoin.dto

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import java.math.BigDecimal
import java.time.Instant

data class ContractSummaryDto(
    val hash: String,
    val confirmedBalance: BigDecimal,
    val confirmedTotalReceived: BigDecimal,
    val confirmedTxNumber: Int,
    val firstActivityDate: Instant,
    val lastActivityDate: Instant,
    val unconfirmedTxValues: Map<String, CqlBitcoinTx> = emptyMap()
) {

    constructor(contract: CqlBitcoinContractSummary, txes: List<CqlBitcoinTx>) : this(
        hash = contract.hash, confirmedBalance = BigDecimal(contract.confirmedBalance),
        confirmedTotalReceived = BigDecimal(contract.confirmedTotalReceived),
        confirmedTxNumber = contract.confirmedTxNumber,
        firstActivityDate = contract.firstActivityDate, lastActivityDate = contract.lastActivityDate,
        unconfirmedTxValues = txes.associateBy { tx -> tx.hash }
    )
}
