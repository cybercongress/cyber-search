package fund.cyber.node.connectors.bitcoin.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.math.BigDecimal


data class Transaction(
        val txid: String,
        val hex: String,
        val version: Int,
        val locktime: Long,
        val vout: List<TransactionOutput>,

        @JsonDeserialize(using = TransactionInputDeserializer::class)
        val vin: List<TransactionInput>
)

data class TransactionOutput(
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
        val hex: String,
        val reqSigs: Int,
        val type: String,
        val addresses: List<String>
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
                        objectMapper.treeToValue(tx, RegularTransactionInput::class.java)
                    }
                }
    }
}