package fund.cyber.node.model

import com.datastax.driver.mapping.annotations.Table
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode
import java.time.Instant


sealed class EthereumItem : CyberSearchItem()

@Table(name = "tx")
data class EthereumTransaction(
        val hash: String,
        val nonce: Long,           //parsed from hex
        val block_hash: String?,   //null when its pending
        val block_number: Long?,   //parsed from hex   //null when its pending
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
) : EthereumItem() {

    fun addressesUsedInTransaction() = listOfNotNull(from, to, creates)
}


@Table(name = "block")
data class EthereumBlock(
        val hash: String,
        val parent_hash: String,
        val number: Long,                   //parsed from hex
        val timestamp: Instant,
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
        val tx_number: Int,
        val uncles: List<String>,
        val block_reward: String,
        val uncles_reward: String,
        val tx_fees: String
) : EthereumItem()


@Table(name = "tx_preview_by_block")
data class EthereumBlockTxPreview(
        val block_number: Long,
        val index: Int,
        val fee: String,
        val value: String,
        val hash: String,
        val from: String,
        val to: String,
        val creates_contract: Boolean
) : EthereumItem() {

    constructor(tx: EthereumTransaction, index: Int) : this(
            block_number = tx.block_number ?: 0,
            index = index, hash = tx.hash,
            fee = tx.fee, value = tx.value,
            from = tx.from, to = (tx.to ?: tx.creates)!!, //both 'to' or 'creates' can't be null at same time
            creates_contract = tx.creates != null
    )
}


@Table(name = "address")
data class EthereumAddress(
        val id: String,
        val balance: String,
        val contract_address: Boolean,
        val total_received: String,
        val last_transaction_block: Long,
        val tx_number: Int
) : EthereumItem()


@Table(name = "tx_preview_by_address")
data class EthereumAddressTxPreview(
        val address: String,
        val fee: String,
        val block_time: Instant,
        val hash: String,
        val from: String,
        val to: String,
        val value: String
) : EthereumItem() {

    constructor(tx: EthereumTransaction, address: String) : this(
            hash = tx.hash, address = address, block_time = tx.block_time,
            from = tx.from, to = (tx.to ?: tx.creates)!!, //both 'to' or 'creates' can't be null at same time
            value = tx.value, fee = tx.fee
    )
}

@Table(name = "mined_block_by_address")
data class EthereumAddressMinedBlock(
        val miner: String,
        val block_number: Long,
        val block_time: Instant,
        val block_reward: BigDecimal,
        val uncles_reward: BigDecimal,
        val tx_fees: BigDecimal,
        val tx_number: Int
) : EthereumItem() {
    constructor(block: EthereumBlock) : this(
            miner = block.miner, block_number = block.number, block_time = block.timestamp,
            block_reward = BigDecimal(block.block_reward), uncles_reward = BigDecimal(block.uncles_reward),
            tx_fees = BigDecimal(block.tx_fees), tx_number = block.tx_number
    )
}


@Table(name = "uncle")
data class EthereumUncle(
        val hash: String, val position: Int,
        val number: Long, val timestamp: Instant,
        val block_number: Long, val block_time: Instant, val block_hash: String,
        val miner: String, val uncle_reward: String
) : EthereumItem()

@Table(name = "uncle_by_address")
data class EthereumAddressUncle(
        val hash: String, val position: Int,
        val number: Long, val timestamp: Instant,
        val block_number: Long, val block_time: Instant, val block_hash: String,
        val miner: String, val uncle_reward: String
) : EthereumItem() {
    constructor(uncle: EthereumUncle) : this(
            hash = uncle.hash, position = uncle.position,
            number = uncle.number, timestamp = uncle.timestamp,
            block_number = uncle.block_number, block_time = uncle.block_time, block_hash = uncle.block_hash,
            miner = uncle.miner, uncle_reward = uncle.uncle_reward
    )
}

private val eight = BigDecimal(8)

fun getUncleReward(uncleNumber: Long, blockNumber: Long, reward: BigDecimal): BigDecimal {
    return ((uncleNumber.toBigDecimal() + eight - blockNumber.toBigDecimal()) * reward)
            .divide(eight, 18, RoundingMode.FLOOR).stripTrailingZeros()
}

fun getBlockReward(number: Long): BigDecimal {
    return if (number < 4370000) BigDecimal("5") else BigDecimal("3")
}