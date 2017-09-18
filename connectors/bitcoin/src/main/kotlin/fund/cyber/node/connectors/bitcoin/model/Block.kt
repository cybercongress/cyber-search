package fund.cyber.node.connectors.bitcoin.model

import java.math.BigDecimal


data class Block(
        val hash: String,
        val confirmations: Int,
        val strippedsize: Int,
        val size: Int,
        val height: Int,
        val weight: Int,
        val version: Int,
        val merkleroot: String,
        val tx: List<Transaction>,
        val time: Long,
        val nonce: Long,
        val bits: Long,
        val difficulty: BigDecimal,
        val previousblockhash: String,
        val nextblockhash: String
)