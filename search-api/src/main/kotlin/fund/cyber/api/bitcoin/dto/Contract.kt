package fund.cyber.api.bitcoin.dto

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractTxPreview
import java.math.BigDecimal
import java.time.Instant

data class ContractTxSummaryDto(
    val hash: String,
    val fee: BigDecimal,
    val inputsNumber: Int,
    val outputsNumber: Int
) {

    constructor(contractTx: CqlBitcoinContractTxPreview) : this(
        fee = contractTx.fee, inputsNumber = contractTx.inputsNumber, hash = contractTx.contractHash,
        outputsNumber = contractTx.outputsNumber
    )
}

data class ContractSummaryDto(
    val hash: String,
    val confirmedBalance: String,
    val confirmedTotalReceived: String,
    val confirmedTxNumber: Int,
    val firstActivityDate: Instant,
    val lastActivityDate: Instant,
    val unconfirmedTxValues: Map<String, ContractTxSummaryDto> = emptyMap()
) {

    constructor(contract: CqlBitcoinContractSummary, txes: List<CqlBitcoinContractTxPreview>) : this(
        hash = contract.hash, confirmedBalance = contract.confirmedBalance,
        confirmedTotalReceived = contract.confirmedTotalReceived, confirmedTxNumber = contract.confirmedTxNumber,
        firstActivityDate = contract.firstActivityDate, lastActivityDate = contract.lastActivityDate,
        unconfirmedTxValues = txes.map { contractTx -> contractTx.hash to ContractTxSummaryDto(contractTx) }.toMap()
    )
}
