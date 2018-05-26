package fund.cyber.api.bitcoin.dto

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTxPreviewIO
import java.math.BigDecimal
import java.time.Instant

data class ContractTxIODto(
    val contracts: List<String>,
    val amount: BigDecimal
) {

    constructor(io: CqlBitcoinTxPreviewIO) : this(
        contracts = io.contracts, amount = io.amount
    )
}

data class ContractTxSummaryDto(
    val fee: BigDecimal,
    val ins: List<ContractTxIODto>,
    val outs: List<ContractTxIODto>
) {

    constructor(contractTx: CqlBitcoinContractTxPreview) : this(
        fee = contractTx.fee, ins = contractTx.ins.map { i -> ContractTxIODto(i) },
        outs = contractTx.outs.map { o -> ContractTxIODto(o) }
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
