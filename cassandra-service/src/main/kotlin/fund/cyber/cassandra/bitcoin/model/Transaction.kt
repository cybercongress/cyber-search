package fund.cyber.cassandra.bitcoin.model

import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.Table
import org.springframework.data.cassandra.core.mapping.UserDefinedType
import java.time.Instant

@Table("tx")
data class CqlBitcoinTx(

        @PrimaryKey val hash: String,
        val block_number: Long,
        val block_hash: String,
        val coinbase: String? = null,
        val block_time: Instant,
        val size: Int,
        val fee: String,
        val total_input: String,
        val total_output: String,
        val ins: List<CqlBitcoinTxIn>,
        val outs: List<CqlBitcoinTxOut>
) : CqlBitcoinItem {

    fun getOutputByNumber(number: Int) = outs.find { out -> out.out == number }!!

    fun allAddressesUsedInTransaction() = ins.flatMap { input -> input.addresses } + outs.flatMap { output -> output.addresses }
}

@UserDefinedType("tx_in")
data class CqlBitcoinTxIn(
        val addresses: List<String>,
        val amount: String,
        val asm: String,
        val tx_id: String,
        val tx_out: Int
)

@UserDefinedType("tx_out")
data class CqlBitcoinTxOut(
        val addresses: List<String>,
        val amount: String,
        val asm: String,
        val out: Int,
        val required_signatures: Int
)