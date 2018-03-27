@file:Suppress("MemberVisibilityCanBePrivate")

package fund.cyber.cassandra.bitcoin.model

import fund.cyber.search.model.bitcoin.BitcoinBlock
import org.springframework.data.cassandra.core.cql.PrimaryKeyType
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.core.mapping.Table
import org.springframework.data.cassandra.core.mapping.UserDefinedType
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant


interface CqlBitcoinItem

@UserDefinedType("tx_preview_io")
data class CqlBitcoinTxPreviewIO(
        val addresses: List<String>,
        val amount: String
)

@Table("tx_preview_by_block")
data class CqlBitcoinBlockTx(
        @PrimaryKeyColumn(name = "block_number", ordinal = 0, type = PrimaryKeyType.PARTITIONED) val blockNumber: Long,
        //specify tx number(order) in block
        @PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED) val index: Int,
        val hash: String,
        val fee: BigDecimal,
        val ins: List<CqlBitcoinTxPreviewIO>,
        val outs: List<CqlBitcoinTxPreviewIO>
) : CqlBitcoinItem

@Table("block")
data class CqlBitcoinBlock(
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
        val total_outputs_value: String
) : CqlBitcoinItem {

    constructor(block: BitcoinBlock) : this(
            height = block.height, hash = block.hash, time = block.time, nonce = block.nonce, bits = block.bits,
            merkleroot = block.merkleroot, size = block.size, version = block.version, weight = block.weight,
            difficulty = block.difficulty, tx_number = block.txNumber,
            total_outputs_value = block.totalOutputsAmount.toString()
    )
}
