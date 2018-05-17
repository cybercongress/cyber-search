package fund.cyber.api.ethereum.dto

import fund.cyber.cassandra.ethereum.model.CqlEthereumContractSummary
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractTxPreview
import java.math.BigDecimal
import java.time.Instant

data class ContractSummaryDto(
    val hash: String,
    val confirmedBalance: String,
    val smartContract: Boolean,
    val confirmedTotalReceived: String,
    val txNumber: Int,
    val minedUncleNumber: Int,
    val minedBlockNumber: Int,
    val firstActivityDate: Instant,
    val lastActivityDate: Instant,
    val unconfirmedTxValues: Map<String, BigDecimal>
) {

    constructor(contract: CqlEthereumContractSummary, txes: List<CqlEthereumContractTxPreview>) : this(
        hash = contract.hash, confirmedBalance = contract.confirmedBalance, smartContract = contract.smartContract,
        confirmedTotalReceived = contract.confirmedTotalReceived, txNumber = contract.txNumber,
        minedUncleNumber = contract.minedUncleNumber, minedBlockNumber = contract.minedBlockNumber,
        firstActivityDate = contract.firstActivityDate, lastActivityDate = contract.lastActivityDate,
        unconfirmedTxValues = txes.map { contractTx -> contractTx.hash to BigDecimal(contractTx.value) }.toMap()
    )
}
