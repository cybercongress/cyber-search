@file:Suppress("MemberVisibilityCanBePrivate")

package fund.cyber.cassandra.ethereum.model

import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import org.springframework.data.cassandra.core.cql.PrimaryKeyType
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.core.mapping.Table
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

interface CqlEthereumItem

@Table("block")
data class CqlEthereumBlock(
        @PrimaryKey val number: Long,
        val hash: String,
        val parent_hash: String,
        val timestamp: Instant,
        @Column("sha3_uncles") val sha3Uncles: String,
        @Column("logs_bloom") val logsBloom: String,
        @Column("transactions_root") val transactionsRoot: String,
        @Column("state_root") val stateRoot: String,
        @Column("receipts_root") val receiptsRoot: String,
        @Column("miner_contract_hash") val minerContractHash: String,
        val difficulty: BigInteger,
        @Column("total_difficulty") val totalDifficulty: BigInteger,
        @Column("extra_data") val extraData: String,
        val size: Long,
        @Column("gas_limit") val gasLimit: Long,
        @Column("gas_used") val gasUsed: Long,
        @Column("tx_number") val txNumber: Int,
        val uncles: List<String>,
        @Column("block_reward") val blockReward: String,
        @Column("uncles_reward") val unclesReward: String,
        @Column("tx_fees") val txFees: String
) : CqlEthereumItem {

    constructor(block: EthereumBlock) : this(
            number = block.number, hash = block.hash, parent_hash = block.parentHash, timestamp = block.timestamp,
            sha3Uncles = block.sha3Uncles, logsBloom = block.logsBloom, transactionsRoot = block.transactionsRoot,
            stateRoot = block.stateRoot, receiptsRoot = block.receiptsRoot, minerContractHash = block.miner,
            difficulty = block.difficulty, totalDifficulty = block.totalDifficulty, extraData = block.extraData,
            size = block.size, gasLimit = block.gasLimit, gasUsed = block.gasUsed, txNumber = block.txNumber,
            uncles = block.uncles, blockReward = block.blockReward.toString(),
            unclesReward = block.unclesReward.toString(), txFees = block.txFees.toString()
    )
}

@Table("tx_preview_by_block")
data class CqlEthereumBlockTxPreview(
        @PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED, value = "block_number")
        val blockNumber: Long,
        @PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED, value = "position_in_block")
        val positionInBlock: Int,
        val fee: BigDecimal,
        val value: BigDecimal,
        val hash: String,
        @Column(forceQuote = true) val from: String,
        @Column(forceQuote = true) val to: String,
        @Column("creates_contract") val createsContract: Boolean
) : CqlEthereumItem {

    //both 'to' or 'createdContract' can't be null at same time
    constructor(tx: EthereumTx) : this(
            blockNumber = tx.blockNumber, hash = tx.hash, positionInBlock = tx.positionInBlock,
            fee = tx.fee, value = tx.value,
            from = tx.from, to = (tx.to ?: tx.createdContract)!!,
            createsContract = tx.createdContract != null
    )
}
