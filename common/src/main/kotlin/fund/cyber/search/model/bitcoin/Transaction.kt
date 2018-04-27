package fund.cyber.search.model.bitcoin

import java.math.BigDecimal
import java.time.Instant

data class BitcoinTx(

    val hash: String,
    val blockNumber: Long,
    val blockHash: String,
    val index: Int,
    val coinbase: String? = null,
    val blockTime: Instant,
    val size: Int,
    val fee: BigDecimal,
    val totalInputsAmount: BigDecimal,
    val totalOutputsAmount: BigDecimal,
    val ins: List<BitcoinTxIn>,
    val outs: List<BitcoinTxOut>
) : BitcoinItem {

    fun getOutputByNumber(number: Int) = outs.find { out -> out.out == number }!!

    fun allContractsUsedInTransaction() = ins.flatMap { input -> input.contracts } +
        outs.flatMap { output -> output.contracts }
}

data class BitcoinTxIn(
    val contracts: List<String>,
    val amount: BigDecimal,
    val asm: String,
    val txHash: String,
    val txOut: Int
)

data class BitcoinTxOut(
    val contracts: List<String>,
    val amount: BigDecimal,
    val asm: String,
    val out: Int,
    val requiredSignatures: Int
)

data class BitcoinCacheTx(
    val txid: String,
    val outs: List<BitcoinCacheTxOutput>
) {

    constructor(tx: JsonRpcBitcoinTransaction) : this(
        txid = tx.txid, outs = tx.vout.map { out -> BitcoinCacheTxOutput(out) }
    )

    fun getOutputByNumber(number: Int) = outs.find { out -> out.n == number }!!
}

data class BitcoinCacheTxOutput(
    val value: BigDecimal,
    val n: Int,
    val addresses: List<String> = listOf("no address")
) {

    constructor(out: JsonRpcBitcoinTransactionOutput) : this(
        value = out.value, n = out.n, addresses = out.scriptPubKey.addresses
    )
}
