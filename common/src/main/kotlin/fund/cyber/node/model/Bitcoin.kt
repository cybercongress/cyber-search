package fund.cyber.node.model

import com.datastax.driver.mapping.annotations.PartitionKey
import com.datastax.driver.mapping.annotations.Table
import com.datastax.driver.mapping.annotations.Transient
import com.datastax.driver.mapping.annotations.UDT
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant


sealed class BitcoinItem : CyberSearchItem()

@UDT(name = "tx_preview_io")
data class BitcoinTransactionPreviewIO(
        val addresses: List<String>,
        val amount: String
)


@Table(name = "tx_preview_by_address")
data class BitcoinAddressTransaction(
        @PartitionKey val address: String,
        val fee: BigDecimal,
        val block_number: Long,
        val block_time: Instant,
        val hash: String,
        val ins: List<BitcoinTransactionPreviewIO>,
        val outs: List<BitcoinTransactionPreviewIO>
) : BitcoinItem()


@Table(name = "tx_preview_by_block")
data class BitcoinBlockTransaction(
        @PartitionKey val block_number: Long,
        val hash: String,
        val index: Int,                     //specify tx number(order) in block
        val fee: BigDecimal,
        val ins: List<BitcoinTransactionPreviewIO>,
        val outs: List<BitcoinTransactionPreviewIO>
) : BitcoinItem()


/*
*
* Bitcoin block
*
*/
@Table(name = "block")
data class BitcoinBlock(
        @PartitionKey val height: Long,
        val hash: String,
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
) : BitcoinItem()


/*
*
* Bitcoin transaction
*
*/
@Table(name = "tx")
data class BitcoinTransaction(
        @PartitionKey val hash: String,
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
) : BitcoinItem() {

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
)


@UDT(name = "tx_out")
data class BitcoinTransactionOut(
        val addresses: List<String>,
        val amount: String,
        val asm: String,
        val out: Int,
        val required_signatures: Int
)

@Table(name = "address")
data class BitcoinAddress(
        @PartitionKey val id: String,
        val confirmed_balance: String,
        val confirmed_total_received: BigDecimal,
        val confirmed_tx_number: Int,
        val unconfirmed_tx_values: Map<String, BigDecimal> = emptyMap()
) : BitcoinItem()