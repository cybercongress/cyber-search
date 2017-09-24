package fund.cyber.node.model

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
        val time: Instant,
        val merkleroot: String,
        val size: Int
) : ItemPreview

data class BitcoinTransactionPreview(
        val hash: String,
        val lock_time: Instant,
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
        val height: BigInteger,
        val time: Instant,
        val nonce: Int,
        val merkleroot: String,
        val size: Int,
        val version: Int,
        val weight: Int,
        val bits: String,
        val difficulty: BigDecimal,
        val txs: List<BitcoinBlockTransaction>
) : Item

data class BitcoinBlockTransaction(
        val fee: BigDecimal,
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

data class BitcoinTransaction(
        val txId: String,
        val block_number: BigInteger,
        val coinbase: String? = null,
        val lock_time: BigInteger, //indicate earliest block when that transaction may be added to the block chain //todo make separate field with time
        val blockTime: Instant,
        val size: Int,
        val fee: BigDecimal,
        val total_input: BigDecimal,
        val total_output: BigDecimal,
        val ins: List<BitcoinTransactionIn>,
        val outs: List<BitcoinTransactionOut>
) : Item {

    fun getOutputByNumber(number: Int): BitcoinTransactionOut = outs.find { out -> out.out == number }!!
}

data class BitcoinTransactionIn(
        val address: String,
        val amount: BigDecimal,
        val asm: String,
        val tx_hash: String,
        val tx_out: Int
)

data class BitcoinTransactionOut(
        val address: String,
        val amount: BigDecimal,
        val asm: String,
        val out: Int,
        val required_signatures: Short
)