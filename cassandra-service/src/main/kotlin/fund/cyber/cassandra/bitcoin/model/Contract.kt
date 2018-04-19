package fund.cyber.cassandra.bitcoin.model

import fund.cyber.cassandra.common.CqlContractSummary
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinTx
import org.springframework.data.cassandra.core.cql.Ordering.DESCENDING
import org.springframework.data.cassandra.core.cql.PrimaryKeyType
import org.springframework.data.cassandra.core.cql.PrimaryKeyType.CLUSTERED
import org.springframework.data.cassandra.core.cql.PrimaryKeyType.PARTITIONED
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant

@Table("contract_summary")
data class CqlBitcoinContractSummary(

        @PrimaryKey override val hash: String,
        @Column("confirmed_balance") val confirmedBalance: BigDecimal,
        @Column("confirmed_total_received") val confirmedTotalReceived: BigDecimal,
        @Column("confirmed_tx_number") val confirmedTxNumber: Int,
        @Column("first_activity_date") val firstActivityDate: Instant,
        @Column("last_activity_date") val lastActivityDate: Instant,
        override val version: Long,
        @Column("kafka_delta_offset") override val kafkaDeltaOffset: Long,
        @Column("kafka_delta_partition") override val kafkaDeltaPartition: Int,
        @Column("kafka_delta_topic") override val kafkaDeltaTopic: String,
        @Column("kafka_delta_offset_committed") override val kafkaDeltaOffsetCommitted: Boolean = false,
        @Column("unconfirmed_tx_values") val unconfirmedTxValues: Map<String, BigDecimal> = emptyMap()
) : CqlBitcoinItem, CqlContractSummary


@Table("tx_preview_by_contract")
data class CqlBitcoinContractTxPreview(

        @PrimaryKeyColumn(ordinal = 0, type = PARTITIONED, value = "contract_hash") val contractHash: String,
        @PrimaryKeyColumn(ordinal = 1, type = CLUSTERED, value = "block_time") val blockTime: Instant,
        @PrimaryKeyColumn(ordinal = 2, type = CLUSTERED) val hash: String,
        val fee: BigDecimal,
        @Column("block_number") val blockNumber: Long,
        val ins: List<CqlBitcoinTxPreviewIO>,
        val outs: List<CqlBitcoinTxPreviewIO>
) : CqlBitcoinItem {

    constructor(contract: String, tx: BitcoinTx) : this(
            contractHash = contract, blockTime = tx.blockTime, hash = tx.blockHash, fee = tx.fee,
            blockNumber = tx.blockNumber, ins = tx.ins.map { txIn -> CqlBitcoinTxPreviewIO(txIn) },
            outs = tx.outs.map { txOut -> CqlBitcoinTxPreviewIO(txOut) }
    )
}

@Table("mined_block_by_contract")
data class CqlBitcoinContractMinedBlock(
        @PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED, value = "miner_contract_hash")
        val minerContractHash: String,
        @PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.CLUSTERED, value = "block_number", ordering = DESCENDING)
        val blockNumber: Long,
        @Column("block_time") val blockTime: Instant,
        @Column("block_reward") val blockReward: BigDecimal,
        @Column("tx_fees") val txFees: BigDecimal,
        @Column("tx_number") val txNumber: Int
) : CqlBitcoinItem {

    constructor(block: BitcoinBlock) : this(
            minerContractHash = block.miner, blockNumber = block.height, blockTime = block.time,
            blockReward = block.blockReward, txFees = block.txFees, txNumber = block.txNumber
    )
}
