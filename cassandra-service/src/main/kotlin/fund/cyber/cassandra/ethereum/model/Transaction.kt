package fund.cyber.cassandra.ethereum.model

import fund.cyber.search.model.ethereum.EthereumTx
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant

@Table("tx")
data class CqlEthereumTransaction(
        @PrimaryKey val hash: String,
        val nonce: Long,
        val block_hash: String?,
        val block_number: Long,
        val block_time: Instant,
        @Column(forceQuote = true) val from: String,
        @Column(forceQuote = true) val to: String?,
        val value: String,
        val gas_price: BigDecimal,
        val gas_limit: Long,
        val gas_used: Long,
        val fee: String,
        val input: String,
        val creates: String?
) : CqlEthereumItem {

    constructor(tx: EthereumTx) : this(
            hash = tx.hash, nonce = tx.nonce, block_hash = tx.blockHash, block_number = tx.blockNumber,
            block_time = tx.blockTime, from = tx.from, to = tx.to,
            value = tx.value.toString(), gas_price = tx.gasPrice, gas_limit = tx.gasLimit, gas_used = tx.gasUsed,
            fee = tx.fee.toString(), input = tx.input, creates = tx.createdContract

    )

    fun addressesUsedInTransaction() = listOfNotNull(from, to, creates)
}
