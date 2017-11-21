package fund.cyber.pump.model

import com.datastax.driver.core.TypeCodec
import com.datastax.driver.extras.codecs.MappingCodec
import fund.cyber.node.model.*
import org.web3j.protocol.core.methods.response.EthBlock
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult
import java.time.format.DateTimeFormatterBuilder
import java.util.*

fun EthereumBlock(pBlock: EthBlock): EthereumBlock {
    val block = pBlock.block

    return fund.cyber.node.model.EthereumBlock(block.hash,
        parent_hash = block.parentHash,
        number = block.number.toLong(),
        timestamp = block.timestamp.toString(),
        sha3_uncles = block.sha3Uncles,
        logs_bloom = block.logsBloom,
        transactions_root = block.transactionsRoot,
        state_root = block.stateRoot,
        receipts_root = block.receiptsRoot,
        miner = block.miner,
        difficulty = block.difficulty,
        total_difficulty = block.totalDifficulty,
        extra_data = block.extraData,
        size = block.size.toLong(),
        gas_limit = block.gasLimit.toLong(),
        gas_used = block.gasUsed.toLong(),
//        transactions = block.transactions.map {
//            it.get() as String
//        },
        tx_number = block.transactions.size,
        uncles = block.uncles,
        block_reward = "",
        tx_fees = ""
    )
}

fun <T>EthereumBlockTransaction_(pBTransaction: TransactionResult<T>) {

}

class TimestampAsStringCodec: MappingCodec<String, Date >(TypeCodec.timestamp(), String::class.java) {
    override fun deserialize(value: Date?): String {
        return value?.toString() ?: ""
    }

    override fun serialize(value: String?): Date {
        return Date()
    }

}

