@file:Suppress("MemberVisibilityCanBePrivate")

package fund.cyber.cassandra.ethereum.model

import fund.cyber.cassandra.common.CqlContractSummary
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.EthereumUncle
import org.springframework.data.cassandra.core.cql.Ordering.DESCENDING
import org.springframework.data.cassandra.core.cql.PrimaryKeyType
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant

@Table("contract_summary")
data class CqlEthereumContractSummary(
        @PrimaryKey override val hash: String,
        @Column("confirmed_balance") val confirmedBalance: BigDecimal,
        @Column("smart_contract") val smartContract: Boolean,
        @Column("confirmed_total_received") val confirmedTotalReceived: BigDecimal,
        @Column("tx_number") val txNumber: Int,
        @Column("uncle_number") val minedUncleNumber: Int,
        @Column("mined_block_number") val minedBlockNumber: Int,
        @Column("first_activity_date") val firstActivityDate: Instant,
        @Column("last_activity_date") val lastActivityDate: Instant,
        override val version: Long,
        @Column("kafka_delta_offset") override val kafkaDeltaOffset: Long,
        @Column("kafka_delta_partition") override val kafkaDeltaPartition: Int,
        @Column("kafka_delta_topic") override val kafkaDeltaTopic: String,
        @Column("kafka_delta_offset_committed") override val kafkaDeltaOffsetCommitted: Boolean = false
) : CqlEthereumItem, CqlContractSummary


@Table("tx_preview_by_contract")
data class CqlEthereumContractTxPreview(
        @PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
        @Column("contract_hash") val contractHash: String,
        @PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED, ordering = DESCENDING, name = "block_time")
        val blockTime: Instant,
        @PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.CLUSTERED) val hash: String,
        val fee: BigDecimal,
        @Column(forceQuote = true) val from: String,
        @Column(forceQuote = true) val to: String,
        val value: String
) : CqlEthereumItem {

    //both 'to' or 'createdSmartContract' can't be null at same time
    constructor(tx: EthereumTx, contractHash: String) : this(
            hash = tx.hash, contractHash = contractHash, blockTime = tx.blockTime,
            from = tx.from, to = (tx.to ?: tx.createdSmartContract)!!,
            value = tx.value.toString(), fee = tx.fee
    )
}

@Table("mined_block_by_contract")
data class CqlEthereumContractMinedBlock(
        @PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED, value = "miner_contract_hash")
        val minerContractHash: String,
        @PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED, value = "block_number", ordering = DESCENDING)
        val blockNumber: Long,
        @Column("block_time") val blockTime: Instant,
        @Column("block_reward") val blockReward: BigDecimal,
        @Column("uncles_reward") val unclesReward: BigDecimal,
        @Column("tx_fees") val txFees: BigDecimal,
        @Column("tx_number") val txNumber: Int
) : CqlEthereumItem {
    constructor(block: EthereumBlock) : this(
            minerContractHash = block.minerContractHash, blockNumber = block.number, blockTime = block.timestamp,
            blockReward = block.blockReward, unclesReward = block.unclesReward,
            txFees = block.txFees, txNumber = block.txNumber
    )
}

@Table("mined_uncle_by_contract")
data class CqlEthereumContractMinedUncle(
        @PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED, value = "miner_contract_hash")
        val minerContractHash: String,
        @PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED, value = "block_number", ordering = DESCENDING)
        val blockNumber: Long,
        val hash: String,
        val position: Int,
        val number: Long,
        val timestamp: Instant,
        @Column("block_time") val blockTime: Instant,
        @Column("block_hash") val blockHash: String,
        @Column("uncle_reward") val uncleReward: String
) : CqlEthereumItem {
    constructor(uncle: EthereumUncle) : this(
            hash = uncle.hash, position = uncle.position,
            number = uncle.number, timestamp = uncle.timestamp,
            blockNumber = uncle.blockNumber, blockTime = uncle.blockTime, blockHash = uncle.blockHash,
            minerContractHash = uncle.miner, uncleReward = uncle.uncleReward.toString()
    )
}
