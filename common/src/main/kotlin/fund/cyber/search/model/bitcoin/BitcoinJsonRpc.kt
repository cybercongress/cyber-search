package fund.cyber.search.model.bitcoin

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.math.BigDecimal

sealed class JsonRpcBitcoinElement

data class JsonRpcBitcoinBlock(
        val hash: String,
        val confirmations: Int,
        val strippedsize: Int,
        val size: Int,
        val height: Long,
        val weight: Int,
        val version: Int,
        val merkleroot: String,
        val rawtx: List<JsonRpcBitcoinTransaction> = emptyList(),
        val tx: List<String> = emptyList(),
        val time: Long,
        val nonce: Long,
        val bits: String,
        val difficulty: BigDecimal,
        val previousblockhash: String?,
        val nextblockhash: String
) : Comparable<JsonRpcBitcoinBlock>, JsonRpcBitcoinElement() {

    override fun compareTo(other: JsonRpcBitcoinBlock) = this.height.compareTo(other.height)
}

data class JsonRpcBitcoinTransaction(
        val txid: String,
        val hex: String,
        val version: Int,
        val size: Int,
        val locktime: Long,
        val vout: List<JsonRpcBitcoinTransactionOutput>,

        @JsonDeserialize(using = TransactionInputDeserializer::class)
        val vin: List<TransactionInput>
) : JsonRpcBitcoinElement() {

    fun getOutputByNumber(number: Int) = vout.find { out -> out.n == number }!!

    fun regularInputs(): List<RegularTransactionInput> =
            vin.filter { input -> input is RegularTransactionInput }
                    .map { input -> input as RegularTransactionInput }
}

data class JsonRpcBitcoinTransactionOutput(
        val value: BigDecimal,
        val n: Int,
        val scriptPubKey: PubKeyScript
)

sealed class TransactionInput


data class CoinbaseTransactionInput(
        val coinbase: String,
        val sequence: Long,
        val txinwitness: String = ""
) : TransactionInput()

data class RegularTransactionInput(
        val txid: String,
        val vout: Int,
        val scriptSig: SignatureScript,
        val sequence: Long,
        val txinwitness: String = ""
) : TransactionInput()

data class PubKeyScript(
        val asm: String,
        val hex: String = "",
        val reqSigs: Int,
        val type: String,
        val addresses: List<String> = listOf("no address")
)

data class SignatureScript(
        val asm: String,
        val hex: String
)

private class TransactionInputDeserializer : JsonDeserializer<List<TransactionInput>>() {

    private val objectMapper = ObjectMapper().registerKotlinModule()

    /**
     * If transaction contains coinbase field -> coinbase transaction
     */
    override fun deserialize(
            jsonParser: JsonParser, deserializationContext: DeserializationContext): List<TransactionInput> {

        val jsonNode: JsonNode = objectMapper.readTree(jsonParser)

        return jsonNode.toList()
                .map { tx ->
                    if (tx["coinbase"] != null) {
                        objectMapper.treeToValue(tx, CoinbaseTransactionInput::class.java)
                    } else {
                        objectMapper.treeToValue(tx, RegularTransactionInput::class.java)
                    }
                }
    }
}