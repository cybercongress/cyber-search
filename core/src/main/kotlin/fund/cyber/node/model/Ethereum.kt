package fund.cyber.node.model

import com.datastax.driver.mapping.annotations.Table
import com.datastax.driver.mapping.annotations.UDT
import java.math.BigDecimal
import java.math.BigInteger


interface EthereumItem

@Table(keyspace = "ethereum", name = "tx",
        readConsistency = "QUORUM", writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false, caseSensitiveTable = false)
data class EthereumTransaction(
        val hash: String,
        val nonce: Long,           //parsed from hex
        val block_hash: String?,   //null when its pending
        val block_number: Long?,   //parsed from hex   //null when its pending
        val transaction_index: Long,//parsed from hex
        val from: String,
        val to: String?,           //null when its a contract creation transaction.
        val value: String,         //decimal   //parsed from hex
        val gas_price: BigDecimal, //parsed from hex
        val gas_limit: Long,       //parsed from hex
        val gas_used: Long,        //parsed from hex
        val fee: String,           //decimal //calculated
        val timestamp: String,     //calculated
        val input: String,
        val creates: String?       //creates contract hash
) : EthereumItem


@Table(keyspace = "ethereum", name = "block",
        readConsistency = "QUORUM", writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false, caseSensitiveTable = false)
data class EthereumBlock(
        val hash: String,
        val parent_hash: String,
        val number: Long,                   //parsed from hex
        val timestamp: String,
        val sha3_uncles: String,
        val logs_bloom: String,
        val transactions_root: String,
        val state_root: String,
        val receipts_root: String,
        val miner: String,
        val difficulty: BigInteger,
        val total_difficulty: BigInteger,   //parsed from hex
        val extra_data: String,
        val size: Long,                     //parsed from hex
        val gas_limit: Long,                //parsed from hex
        val gas_used: Long,                //parsed from hex
        val transactions: List<EthereumBlockTransaction>,
        val tx_number: Int,
        val uncles: List<String>,
        val block_reward: String,
        val tx_fees: String
) : EthereumItem {

    fun addressesUsedInBlock() = transactions.flatMap { tx -> tx.addressesUsedInTransaction() }.plus(miner)
}


@UDT(name = "ethereum.block_tx")
data class EthereumBlockTransaction(
        val fee: BigDecimal,
        val amount: BigDecimal,
        val hash: String,
        val from: String,
        val to: String,
        val creates_contract: Boolean
) {
    //used by gson to create instance
    constructor() : this(BigDecimal.ZERO, BigDecimal.ZERO, "", "", "", false)

    fun addressesUsedInTransaction() = listOf(from, to)
}


@Table(keyspace = "ethereum", name = "address",
        readConsistency = "QUORUM", writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false, caseSensitiveTable = false)
data class EthereumAddress(
        val id: String,
        val balance: String,
        val is_contract_address: Boolean,
        val total_received: String,
        val last_transaction_block: Long,
        val tx_number: Int
) : EthereumItem


@Table(keyspace = "ethereum", name = "tx_preview_by_address",
        readConsistency = "QUORUM", writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false, caseSensitiveTable = false)
data class EthereumAddressTransaction(
        val address: String,
        val fee: String,
        val block_time: String,
        val hash: String,
        val from: String,
        val to: String,
        val value: String
) : EthereumItem


fun getBlockReward(number: Long): BigDecimal {
    return if (number < 4370000) BigDecimal("5") else BigDecimal("3")
}