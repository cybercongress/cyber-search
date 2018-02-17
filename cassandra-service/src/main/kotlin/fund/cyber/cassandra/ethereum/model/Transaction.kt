package fund.cyber.cassandra.ethereum.model

import fund.cyber.search.model.ethereum.EthereumTransaction
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
        val transaction_index: Long,
        val from: String,
        val to: String?,
        val value: String,
        val gas_price: BigDecimal,
        val gas_limit: Long,
        val gas_used: Long,
        val fee: String,
        val input: String,
        val creates: String?
) : CqlEthereumItem {

    constructor(tx: EthereumTransaction) : this(
            hash = tx.hash, nonce = tx.nonce, block_hash = tx.block_hash, block_number = tx.block_number,
            block_time = tx.block_time, transaction_index = tx.transaction_index, from = tx.from, to = tx.to,
            value = tx.value.toString(), gas_price = tx.gas_price, gas_limit = tx.gas_limit, gas_used = tx.gas_used,
            fee = tx.fee.toString(), input = tx.input, creates = tx.creates

    )

    fun addressesUsedInTransaction() = listOfNotNull(from, to, creates)
}
