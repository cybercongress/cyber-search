@file:Suppress("MemberVisibilityCanBePrivate")

package fund.cyber.cassandra.ethereum.model

import fund.cyber.search.model.ethereum.EthereumTx
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant

@Table("tx")
data class CqlEthereumTx(
        @PrimaryKey val hash: String,
        val nonce: Long,
        @Column("block_hash") val blockHash: String?,
        @Column("block_number") val blockNumber: Long,
        @Column("block_time") val blockTime: Instant,
        @Column(forceQuote = true) val from: String,
        @Column(forceQuote = true) val to: String?,
        val value: String,
        @Column("gas_price") val gasPrice: BigDecimal,
        @Column("gas_limit") val gasLimit: Long,
        @Column("gas_used") val gasUsed: Long,
        val fee: String,
        val input: String,
        @Column("created_contract") val createdContract: String?
) : CqlEthereumItem {

    constructor(tx: EthereumTx) : this(
            hash = tx.hash, nonce = tx.nonce, blockHash = tx.blockHash, blockNumber = tx.blockNumber,
            blockTime = tx.blockTime, from = tx.from, to = tx.to,
            value = tx.value.toString(), gasPrice = tx.gasPrice, gasLimit = tx.gasLimit, gasUsed = tx.gasUsed,
            fee = tx.fee.toString(), input = tx.input, createdContract = tx.createdSmartContract

    )

    fun addressesUsedInTransaction() = listOfNotNull(from, to, createdContract)
}
