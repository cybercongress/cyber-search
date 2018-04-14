@file:Suppress("MemberVisibilityCanBePrivate")

package fund.cyber.cassandra.bitcoin.model

import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.BitcoinTxIn
import fund.cyber.search.model.bitcoin.BitcoinTxOut
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.Table
import org.springframework.data.cassandra.core.mapping.UserDefinedType
import java.math.BigDecimal
import java.time.Instant

@Table("tx")
data class CqlBitcoinTx(

        @PrimaryKey val hash: String,
        val block_number: Long,
        val block_hash: String,
        val coinbase: String? = null,
        val block_time: Instant,
        val size: Int,
        val fee: String,
        val total_input: String,
        val total_output: String,
        val ins: List<CqlBitcoinTxIn>,
        val outs: List<CqlBitcoinTxOut>
) : CqlBitcoinItem {

    constructor(tx: BitcoinTx) : this(
            hash = tx.hash, block_number = tx.blockNumber, block_hash = tx.blockHash, coinbase = tx.coinbase,
            block_time = tx.blockTime, size = tx.size, fee = tx.fee.toString(),
            total_input = tx.totalInputsAmount.toString(), total_output = tx.totalOutputsAmount.toString(),
            ins = tx.ins.map { txIn -> CqlBitcoinTxIn(txIn) }, outs = tx.outs.map { txOut -> CqlBitcoinTxOut(txOut) }
    )

    fun getOutputByNumber(number: Int) = outs.find { out -> out.out == number }!!

    fun allAddressesUsedInTransaction() = ins.flatMap { input -> input.addresses } +
            outs.flatMap { output -> output.addresses }
}

@UserDefinedType("tx_in")
data class CqlBitcoinTxIn(
        val addresses: List<String>,
        val amount: BigDecimal,
        val asm: String,
        val tx_hash: String,
        val tx_out: Int
) {

    constructor(txIn: BitcoinTxIn) : this(
            addresses = txIn.addresses, amount = txIn.amount, asm = txIn.asm,
            tx_hash = txIn.txHash, tx_out = txIn.txOut
    )
}

@UserDefinedType("tx_out")
data class CqlBitcoinTxOut(
        val addresses: List<String>,
        val amount: BigDecimal,
        val asm: String,
        val out: Int,
        val required_signatures: Int
) {

    constructor(txOut: BitcoinTxOut) : this (
            addresses = txOut.addresses, amount = txOut.amount, asm = txOut.asm,
            out = txOut.out, required_signatures = txOut.requiredSignatures
    )
}
