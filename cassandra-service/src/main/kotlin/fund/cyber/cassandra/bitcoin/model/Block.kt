@file:Suppress("MemberVisibilityCanBePrivate")

package fund.cyber.cassandra.bitcoin.model

import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.BitcoinTxIn
import fund.cyber.search.model.bitcoin.BitcoinTxOut
import org.springframework.data.cassandra.core.cql.PrimaryKeyType
import org.springframework.data.cassandra.core.mapping.Column
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
        val amount: BigDecimal
) {

    constructor(txIn: BitcoinTxIn) : this(
            addresses = txIn.addresses, amount = txIn.amount
    )

    constructor(txOut: BitcoinTxOut) : this(
            addresses = txOut.addresses, amount = txOut.amount
    )
}

@Table("tx_preview_by_block")
data class CqlBitcoinBlockTxPreview(
        @PrimaryKeyColumn(name = "block_number", ordinal = 0, type = PrimaryKeyType.PARTITIONED) val blockNumber: Long,
        //specify tx number(order) in block
        @PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED) val index: Int,
        val hash: String,
        val fee: BigDecimal,
        val ins: List<CqlBitcoinTxPreviewIO>,
        val outs: List<CqlBitcoinTxPreviewIO>
) : CqlBitcoinItem {

    constructor(tx: BitcoinTx) : this(
            blockNumber = tx.blockNumber, index = tx.index, hash = tx.hash, fee = tx.fee,
            ins = tx.ins.map { txIn -> CqlBitcoinTxPreviewIO(txIn) },
            outs = tx.outs.map { txOut -> CqlBitcoinTxPreviewIO(txOut) }
    )
}

@Table("block")
data class CqlBitcoinBlock(
        @PrimaryKey val height: Long,
        val hash: String,
        val miner: String,
        @Column("block_reward") val blockReward: BigDecimal,
        @Column("tx_fees") val txFees: BigDecimal,
        @Column("coinbase_data") val coinbaseData: String,
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
            height = block.height, hash = block.hash, miner = block.miner, blockReward = block.blockReward,
            txFees = block.txFees, coinbaseData = block.coinbaseData, time = block.time, nonce = block.nonce,
            bits = block.bits, merkleroot = block.merkleroot, size = block.size, version = block.version,
            weight = block.weight, difficulty = block.difficulty, tx_number = block.txNumber,
            total_outputs_value = block.totalOutputsAmount.toString()
    )
}
