package fund.cyber.search.model.bitcoin

import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant


interface BitcoinItem

data class BitcoinTxPreviewIO(
        val addresses: List<String>,
        val amount: BigDecimal
)

data class BitcoinBlockTx(
        val blockNumber: Long,
        val index: Int,         //specify tx number(order) in block
        val hash: String,
        val fee: BigDecimal,
        val ins: List<BitcoinTxPreviewIO>,
        val outs: List<BitcoinTxPreviewIO>
) : BitcoinItem


data class BitcoinBlock(
        val height: Long,
        val hash: String,
        val time: Instant,
        val nonce: Long,
        val merkleroot: String,
        val size: Int,
        val version: Int,
        val weight: Int,
        val bits: String,
        val difficulty: BigInteger,
        val txNumber: Int,
        val totalOutputsAmount: BigDecimal
) : BitcoinItem
