@file:Suppress("MemberVisibilityCanBePrivate")

package fund.cyber.cassandra.bitcoin.model

import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.BitcoinTxIn
import fund.cyber.search.model.bitcoin.BitcoinTxOut
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.Table
import org.springframework.data.cassandra.core.mapping.UserDefinedType
import java.math.BigDecimal
import java.time.Instant

@Table("tx")
data class CqlBitcoinTx(

        @PrimaryKey val hash: String,
        @Column("block_number") val blockNumber: Long,
        @Column("block_hash") val blockHash: String,
        val coinbase: String? = null,
        @Column("block_time") val blockTime: Instant,
        val size: Int,
        val fee: String,
        @Column("total_input") val totalInput: String,
        @Column("total_output") val totalOutput: String,
        val ins: List<CqlBitcoinTxIn>,
        val outs: List<CqlBitcoinTxOut>
) : CqlBitcoinItem {

    constructor(tx: BitcoinTx) : this(
            hash = tx.hash, blockNumber = tx.blockNumber, blockHash = tx.blockHash, coinbase = tx.coinbase,
            blockTime = tx.blockTime, size = tx.size, fee = tx.fee.toString(),
            totalInput = tx.totalInputsAmount.toString(), totalOutput = tx.totalOutputsAmount.toString(),
            ins = tx.ins.map { txIn -> CqlBitcoinTxIn(txIn) }, outs = tx.outs.map { txOut -> CqlBitcoinTxOut(txOut) }
    )

    fun getOutputByNumber(number: Int) = outs.find { out -> out.out == number }!!

    fun allContractsUsedInTransaction() = ins.flatMap { input -> input.contracts } +
            outs.flatMap { output -> output.contracts }
}

@UserDefinedType("tx_in")
data class CqlBitcoinTxIn(
        val contracts: List<String>,
        val amount: BigDecimal,
        val asm: String,
        @Column("tx_hash") val txHash: String,
        @Column("tx_out") val txOut: Int
) {

    constructor(txIn: BitcoinTxIn) : this(
            contracts = txIn.contracts, amount = txIn.amount, asm = txIn.asm,
            txHash = txIn.txHash, txOut = txIn.txOut
    )
}

@UserDefinedType("tx_out")
data class CqlBitcoinTxOut(
        val contracts: List<String>,
        val amount: BigDecimal,
        val asm: String,
        val out: Int,
        @Column("required_signatures") val requiredSignatures: Int
) {

    constructor(txOut: BitcoinTxOut) : this (
            contracts = txOut.contracts, amount = txOut.amount, asm = txOut.asm,
            out = txOut.out, requiredSignatures = txOut.requiredSignatures
    )
}
