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

    fun allAddressesUsedInTransaction() = ins.flatMap { input -> input.addresses } +
            outs.flatMap { output -> output.addresses }
}

data class BitcoinTxIn(
        val addresses: List<String>,
        val amount: BigDecimal,
        val asm: String,
        val txHash: String,
        val txOut: Int
)

data class BitcoinTxOut(
        val addresses: List<String>,
        val amount: BigDecimal,
        val asm: String,
        val out: Int,
        val requiredSignatures: Int
)
