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
        val bits: Int,
        val difficulty: BigInteger,
        val txs: List<BitcoinBlockTransaction>
) : Item

data class BitcoinBlockTransaction(
        val fee: BigDecimal,
        val lock_time: Instant,
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
        val hash: String,
        val block_number: BigInteger,
        val coinbase: String?,
        val lock_time: Instant,
        val fee: BigDecimal,
        val total_input: BigDecimal,
        val total_output: BigDecimal,
        val ins: List<BitcoinTransactionIn>,
        val outs: List<BitcoinTransactionOut>
) : Item

data class BitcoinTransactionIn(
        val address: String,
        val amount: BigDecimal,
        val asm: String,
        val tx_hash: String,
        val tx_out: String
)

data class BitcoinTransactionOut(
        val address: String,
        val amount: BigDecimal,
        val asm: String,
        val out: Short,
        val required_signatures: Short
)