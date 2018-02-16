package fund.cyber.cassandra.ethereum.model

import fund.cyber.search.model.ethereum.EthereumTransaction
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant

@Table("tx")
data class CqlEthereumTransaction(
        @PrimaryKey val hash: String,
        val nonce: Long,           //parsed from hex
        val block_hash: String?,   //null when its pending
        val block_number: Long,   //parsed from hex   //null when its pending
        val block_time: Instant,
        val transaction_index: Long,//parsed from hex
        val from: String,
        val to: String?,           //null when its a contract creation transaction.
        val value: String,         //decimal   //parsed from hex
        val gas_price: BigDecimal, //parsed from hex
        val gas_limit: Long,       //parsed from hex
        val gas_used: Long,        //parsed from hex
        val fee: String,           //decimal //calculated
        val input: String,
        val creates: String?       //creates contract hash
) : CqlEthereumItem {

    constructor(tx: EthereumTransaction) : this(
            hash = tx.hash, nonce = tx.nonce, block_hash = tx.block_hash, block_number = tx.block_number,
            block_time = tx.block_time, transaction_index = tx.transaction_index, from = tx.from, to = tx.to,
            value = tx.value, gas_price = tx.gas_price, gas_limit = tx.gas_limit, gas_used = tx.gas_used,
            fee = tx.fee, input = tx.input, creates = tx.creates
    )

    fun addressesUsedInTransaction() = listOfNotNull(from, to, creates)
}
