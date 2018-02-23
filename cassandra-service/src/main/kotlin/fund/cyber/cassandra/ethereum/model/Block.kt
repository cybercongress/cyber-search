package fund.cyber.cassandra.ethereum.model

import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
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
        val sha3_uncles: String,
        val logs_bloom: String,
        val transactions_root: String,
        val state_root: String,
        val receipts_root: String,
        val miner: String,
        val difficulty: BigInteger,
        val total_difficulty: BigInteger,
        val extra_data: String,
        val size: Long,
        val gas_limit: Long,
        val gas_used: Long,
        val tx_number: Int,
        val uncles: List<String>,
        val block_reward: String,
        val uncles_reward: String,
        val tx_fees: String
) : CqlEthereumItem {

    constructor(block: EthereumBlock) : this(
            number = block.number, hash = block.hash, parent_hash = block.parentHash, timestamp = block.timestamp,
            sha3_uncles = block.sha3Uncles, logs_bloom = block.logsBloom, transactions_root = block.transactionsRoot,
            state_root = block.stateRoot, receipts_root = block.receiptsRoot, miner = block.miner,
            difficulty = block.difficulty, total_difficulty = block.totalDifficulty, extra_data = block.extraData,
            size = block.size, gas_limit = block.gasLimit, gas_used = block.gasUsed, tx_number = block.txNumber,
            uncles = block.uncles, block_reward = block.blockReward.toString(),
            uncles_reward = block.unclesReward.toString(), tx_fees = block.txFees.toString()
    )
}

@Table("tx_preview_by_block")
data class CqlEthereumBlockTxPreview(
        @PrimaryKey(value = "block_number") val blockNumber: Long,
        @Column("position_in_block")  val positionInBlock: Int,
        val fee: BigDecimal,
        val value: BigDecimal,
        val hash: String,
        @Column(forceQuote = true) val from: String,
        @Column(forceQuote = true) val to: String,
        val creates_contract: Boolean
) : CqlEthereumItem {

    constructor(tx: EthereumTx) : this(
            blockNumber = tx.blockNumber, hash = tx.hash, positionInBlock = tx.positionInBlock,
            fee = tx.fee, value = tx.value,
            from = tx.from, to = (tx.to ?: tx.createdContract)!!, //both 'to' or 'createdContract' can't be null at same time
            creates_contract = tx.createdContract != null
    )
}