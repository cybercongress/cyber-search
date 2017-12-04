package fund.cyber.node.model

import com.datastax.driver.mapping.annotations.Table
import com.datastax.driver.mapping.annotations.Transient
import com.datastax.driver.mapping.annotations.UDT
import java.math.BigInteger
import java.time.Instant


interface BitcoinItem

@UDT(name = "tx_preview_io")
data class BitcoinTransactionPreviewIO(
        val addresses: List<String>,
        val amount: String
) {
    //used by gson to create instance
    constructor() : this(emptyList(), "")
}


@Table(name = "tx_preview_by_address", readConsistency = "QUORUM", writeConsistency = "QUORUM")
data class BitcoinAddressTransaction(
        val address: String,
        val fee: String,
        val block_time: Instant,
        val hash: String,
        val ins: List<BitcoinTransactionPreviewIO>,
        val outs: List<BitcoinTransactionPreviewIO>
) : BitcoinItem


@Table(name = "tx_preview_by_block", readConsistency = "QUORUM", writeConsistency = "QUORUM")
data class BitcoinBlockTransaction(
        val hash: String,
        val block_number: Long,
        val fee: String,
        val ins: List<BitcoinTransactionPreviewIO>,
        val outs: List<BitcoinTransactionPreviewIO>
) : BitcoinItem


/*
*
* Bitcoin block
*
*/
@Table(name = "block", readConsistency = "QUORUM", writeConsistency = "QUORUM")
data class BitcoinBlock(
        val hash: String,
        val height: Long,
        val time: Instant,
        val nonce: Long,
        val merkleroot: String,
        val size: Int,
        val version: Int,
        val weight: Int,
        val bits: String,
        val difficulty: BigInteger,
        val tx_number: Int,
        val total_outputs_value: String,
        @Transient val transactionPreviews: List<BitcoinBlockTransaction> = emptyList()
) : BitcoinItem


/*
*
* Bitcoin transaction
*
*/
@Table(name = "tx", readConsistency = "QUORUM", writeConsistency = "QUORUM")
data class BitcoinTransaction(
        val txid: String,
        val block_number: Long,
        val block_hash: String,
        val coinbase: String? = null,
        val block_time: Instant,
        val size: Int,
        val fee: String,
        val total_input: String,
        val total_output: String,
        val ins: List<BitcoinTransactionIn>,
        val outs: List<BitcoinTransactionOut>
) : BitcoinItem {

    fun getOutputByNumber(number: Int) = outs.find { out -> out.out == number }!!

    fun allAddressesUsedInTransaction() = ins.flatMap { input -> input.addresses } + outs.flatMap { output -> output.addresses }
}


@UDT(name = "tx_in")
data class BitcoinTransactionIn(
        val addresses: List<String>,
        val amount: String,
        val asm: String,
        val tx_id: String,
        val tx_out: Int
) {

    //used by gson to create instance
    constructor() : this(emptyList(), "0", "", "", 0)
}


@UDT(name = "tx_out")
data class BitcoinTransactionOut(
        val addresses: List<String>,
        val amount: String,
        val asm: String,
        val out: Int,
        val required_signatures: Int
) {

    //used by gson to create instance
    constructor() : this(emptyList(), "0", "", 0, 1)
}


@Table(name = "address", readConsistency = "QUORUM", writeConsistency = "QUORUM")
data class BitcoinAddress(
        val id: String,
        val balance: String,
        val total_received: String,
        val last_transaction_block: Long,
        val tx_number: Int
) : BitcoinItem