package cyber.search.bitcoin.model

import org.springframework.data.annotation.Transient
import org.springframework.data.cassandra.core.cql.PrimaryKeyType
import org.springframework.data.cassandra.core.mapping.*
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant


interface BitcoinItem

@UserDefinedType("tx_preview_io")
data class BitcoinTransactionPreviewIO(
        val addresses: List<String>,
        val amount: String
)

@Table("tx_preview_by_block")
data class BitcoinBlockTransaction(
        @PrimaryKeyColumn(name = "block_number", ordinal = 0, type = PrimaryKeyType.PARTITIONED) val blockNumber: Long,
        @PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED) val index: Int, //specify tx number(order) in block
        val hash: String,
        val fee: BigDecimal,
        val ins: List<BitcoinTransactionPreviewIO>,
        val outs: List<BitcoinTransactionPreviewIO>
) : BitcoinItem

@Table("block")
data class BitcoinBlock(
        @PrimaryKey val height: Long,
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
) : BitcoinItem