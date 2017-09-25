package fund.cyber.node.model

import com.datastax.driver.mapping.annotations.Table
import com.datastax.driver.mapping.annotations.UDT
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

/*
*
* Items Previews
*
*/

data class BitcoinBlockPreview(
        val hash: String,
        val height: BigInteger,
        val time: String,
        val merkleroot: String,
        val size: Int
) : ItemPreview

data class BitcoinTransactionPreview(
        val hash: String,
        val lock_time: Long,
        val block_number: BigInteger,
        val fee: BigDecimal
) : ItemPreview

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
        val txs: List<BitcoinBlockTransaction>
) : Item

data class BitcoinBlockTransaction(
        val fee: String,
        val lock_time: Instant,
        val hash: String,
        val ins: List<BitcoinBlockTransactionIO>,
        val outs: List<BitcoinBlockTransactionIO>
)

data class BitcoinBlockTransactionIO(
        val address: String,
        val amount: BigDecimal
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
        val coinbase: String? = null,
        val lock_time: Long, //indicate earliest block when that transaction may be added to the block chain //todo make separate field with time
        val blockTime: String,
        val size: Int,
        val fee: String,
        val total_input: String,
        val total_output: String,
        val ins: List<BitcoinTransactionIn>,
        val outs: List<BitcoinTransactionOut>
) : Item {

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

    constructor() : this("", "0", "", 0, 1)
}