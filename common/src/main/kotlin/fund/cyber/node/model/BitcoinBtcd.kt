package fund.cyber.node.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.math.BigDecimal

sealed class BtcdBitcoinElement

data class BtcdBlock(
        val hash: String,
        val confirmations: Int,
        val strippedsize: Int,
        val size: Int,
        val height: Long,
        val weight: Int,
        val version: Int,
        val merkleroot: String,
        val rawtx: List<BtcdTransaction>,
        val time: Long,
        val nonce: Long,
        val bits: String,
        val difficulty: BigDecimal,
        val previousblockhash: String,
        val nextblockhash: String
) : BtcdBitcoinElement()

data class BtcdTransaction(
        val txid: String,
        val hex: String,
        val version: Int,
        val size: Int,
        val locktime: Long,
        val vout: List<BtcdTransactionOutput>,

        @JsonDeserialize(using = TransactionInputDeserializer::class)
        val vin: List<TransactionInput>
) : BtcdBitcoinElement() {

    fun regularInputs(): List<BtcdRegularTransactionInput> =
            vin.filter { input -> input is BtcdRegularTransactionInput }
                    .map { input -> input as BtcdRegularTransactionInput }
}

data class BtcdTransactionOutput(
        val value: String,
        val n: Int,
        val scriptPubKey: PubKeyScript
)

sealed class TransactionInput
data class CoinbaseTransactionInput(
        val coinbase: String,
        val sequence: Long,
        val txinwitness: String = ""
) : TransactionInput()

data class BtcdRegularTransactionInput(
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

class TransactionInputDeserializer : JsonDeserializer<List<TransactionInput>>() {

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
                        objectMapper.treeToValue(tx, BtcdRegularTransactionInput::class.java)
                    }
                }
    }
}