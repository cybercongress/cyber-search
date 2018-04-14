package fund.cyber.cassandra.bitcoin.model

import fund.cyber.cassandra.common.CqlAddressSummary
import fund.cyber.search.model.bitcoin.BitcoinTx
import org.springframework.data.cassandra.core.cql.PrimaryKeyType.CLUSTERED
import org.springframework.data.cassandra.core.cql.PrimaryKeyType.PARTITIONED
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn
import org.springframework.data.cassandra.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant

@Table("address_summary")
data class CqlBitcoinAddressSummary(

        @PrimaryKey override val id: String,
        @Column("confirmed_balance") val confirmedBalance: BigDecimal,
        @Column("confirmed_total_received") val confirmedTotalReceived: BigDecimal,
        @Column("confirmed_tx_number") val confirmedTxNumber: Int,
        override val version: Long,
        @Column("kafka_delta_offset") override val kafkaDeltaOffset: Long,
        @Column("kafka_delta_partition") override val kafkaDeltaPartition: Int,
        @Column("kafka_delta_topic") override val kafkaDeltaTopic: String,
        @Column("kafka_delta_offset_committed") override val kafkaDeltaOffsetCommitted: Boolean = false,
        @Column("unconfirmed_tx_values") val unconfirmedTxValues: Map<String, BigDecimal> = emptyMap()
) : CqlBitcoinItem, CqlAddressSummary


@Table("tx_preview_by_address")
data class CqlBitcoinAddressTx(

        @PrimaryKeyColumn(ordinal = 0, type = PARTITIONED) val address: String,
        @PrimaryKeyColumn(ordinal = 1, type = CLUSTERED) val block_time: Instant,
        @PrimaryKeyColumn(ordinal = 2, type = CLUSTERED) val hash: String,
        val fee: BigDecimal,
        val block_number: Long,
        val ins: List<CqlBitcoinTxPreviewIO>,
        val outs: List<CqlBitcoinTxPreviewIO>
) : CqlBitcoinItem {

    constructor(address: String, tx: BitcoinTx) : this(
            address = address, block_time = tx.blockTime, hash = tx.blockHash, fee = tx.fee,
            block_number = tx.blockNumber, ins = tx.ins.map { txIn -> CqlBitcoinTxPreviewIO(txIn) },
            outs = tx.outs.map { txOut -> CqlBitcoinTxPreviewIO(txOut) }
    )
}
