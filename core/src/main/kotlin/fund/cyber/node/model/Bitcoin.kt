package fund.cyber.node.model

import com.datastax.driver.mapping.annotations.Table
import com.datastax.driver.mapping.annotations.UDT
import java.math.BigDecimal


interface BitcoinItem

/*
*
* Bitcoin block
*
*/

data class BitcoinBlock(
        val hash: String,
        val height: Long,
        val time: String,
        val nonce: Long,
        val merkleroot: String,
        val size: Int,
        val version: Int,
        val weight: Int,
        val bits: String,
        val difficulty: BigDecimal,
        val tx_number : Int,
        val total_outputs_value: String,
        val txs: List<BitcoinBlockTransaction>
) : BitcoinItem

data class BitcoinBlockTransaction(
        val fee: String,
        val lock_time: Long,
        val hash: String,
        val ins: List<BitcoinBlockTransactionIO>,
        val outs: List<BitcoinBlockTransactionIO>
)

data class BitcoinBlockTransactionIO(
        val address: String,
        val amount: String
)


/*
*
* Bitcoin transaction
*
*/
@Table(keyspace = "blockchains", name = "bitcoin_tx",
        readConsistency = "QUORUM", writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false, caseSensitiveTable = false)
data class BitcoinTransaction(
        val txId: String,
        val block_number: Long,
        val block_hash : String,
        val coinbase: String? = null,
        val lock_time: Long, //indicate earliest block when that transaction may be added to the block chain //todo make separate field with time
        val block_time: String,
        val size: Int,
        val fee: String,
        val total_input: String,
        val total_output: String,
        val ins: List<BitcoinTransactionIn>,
        val outs: List<BitcoinTransactionOut>
) : BitcoinItem {

    fun getOutputByNumber(number: Int): BitcoinTransactionOut = outs.find { out -> out.out == number }!!
}

@UDT(name = "bitcoin_tx_in")
data class BitcoinTransactionIn(
        val address: String,
        val amount: String,
        val asm: String,
        val tx_id: String,
        val tx_out: Int
) {

    //used by gson to create instance
    constructor() : this("", "0", "", "", 0)
}

@UDT(name = "bitcoin_tx_out")
data class BitcoinTransactionOut(
        val address: String,
        val amount: String,
        val asm: String,
        val out: Int,
        val required_signatures: Int
) {

    //used by gson to create instance
    constructor() : this("", "0", "", 0, 1)
}