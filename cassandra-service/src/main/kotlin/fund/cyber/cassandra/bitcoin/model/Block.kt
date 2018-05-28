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
        val contracts: List<String>,
        val amount: BigDecimal
) {

    constructor(txIn: BitcoinTxIn) : this(
            contracts = txIn.contracts, amount = txIn.amount
    )

    constructor(txOut: BitcoinTxOut) : this(
            contracts = txOut.contracts, amount = txOut.amount
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
    @PrimaryKey val number: Long,
    val hash: String,
    @Column("parent_hash") val parentHash: String,
    @Column("miner_contract_hash") val minerContractHash: String,
    @Column("block_reward") val blockReward: BigDecimal,
    @Column("tx_fees") val txFees: BigDecimal,
    @Column("coinbase_data") val coinbaseData: String,
    val timestamp: Instant,
    val nonce: Long,
    val merkleroot: String,
    val size: Int,
    val version: Int,
    val weight: Int,
    val bits: String,
    val difficulty: BigInteger,
    @Column("tx_number") val txNumber: Int,
    @Column("total_outputs_value") val totalOutputsValue: String
) : CqlBitcoinItem {

    constructor(block: BitcoinBlock) : this(
            number = block.height, hash = block.hash, parentHash = block.parentHash,
            minerContractHash = block.minerContractHash,
            blockReward = block.blockReward, txFees = block.txFees, coinbaseData = block.coinbaseData,
            timestamp = block.time, nonce = block.nonce, bits = block.bits, merkleroot = block.merkleroot,
            size = block.size, version = block.version, weight = block.weight, difficulty = block.difficulty,
            txNumber = block.txNumber, totalOutputsValue = block.totalOutputsAmount.toString()
    )
}
