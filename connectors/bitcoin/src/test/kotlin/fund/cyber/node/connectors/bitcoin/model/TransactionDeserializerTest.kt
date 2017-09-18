package fund.cyber.node.connectors.bitcoin.model

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal


private val rawTransaction = """
{
  "hex"     : "01000000010000000000000000000000000000000000000000000000000000000000000000f...",
  "txid"    : "90743aad855880e517270550d2a881627d84db5265142fd1e7fb7add38b08be9",
  "version" : 1,
  "locktime": 0,
  "vin"     : [
    {
      "txid"     : "60ac4b057247b3d0b9a8173de56b5e1be8c1d1da970511c626ef53706c66be04",
      "vout"     : 0,
      "scriptSig": {
        "asm": "3046022100cb42f8df44eca83dd0a727988dcde9384953e830b1f8004d57485e2ede1b9c8f0...",
        "hex": "493046022100cb42f8df44eca83dd0a727988dcde9384953e830b1f8004d57485e2ede1b9c8..."
      },
      "sequence" : 4294967295
    },
    {
      "coinbase"   : "60ac4b057247b3d0b9a8173de56b5e1be8c1d1da970511c626ef53706c66be04",
      "sequence"   : 4294967295,
      "txinwitness": "data"
    }
  ],
  "vout"    : [
    {
      "value"       : 25.1394,
      "n"           : 0,
      "scriptPubKey": {
        "asm"      : "OP_DUP OP_HASH160 ea132286328cfc819457b9dec386c4b5c84faa5c OP_EQUALVERIFY OP_CHECKSIG",
        "hex"      : "76a914ea132286328cfc819457b9dec386c4b5c84faa5c88ac",
        "reqSigs"  : 1,
        "type"     : "pubkeyhash",
        "addresses": [
          "1NLg3QJMsMQGM5KEUaEu5ADDmKQSLHwmyh"
        ]
      }
    }
  ]
}
"""

private val regularVin = RegularTransactionInput(
        txid = "60ac4b057247b3d0b9a8173de56b5e1be8c1d1da970511c626ef53706c66be04",
        vout = 0, sequence = 4294967295,
        scriptSig = SignatureScript(
                asm = "3046022100cb42f8df44eca83dd0a727988dcde9384953e830b1f8004d57485e2ede1b9c8f0...",
                hex = "493046022100cb42f8df44eca83dd0a727988dcde9384953e830b1f8004d57485e2ede1b9c8..."
        )
)

private val coinbaseVin = CoinbaseTransactionInput(
        coinbase = "60ac4b057247b3d0b9a8173de56b5e1be8c1d1da970511c626ef53706c66be04",
        sequence = 4294967295, txinwitness = "data"
)

private val regularOut = TransactionOutput(
        value = BigDecimal("25.1394"), n = 0,
        scriptPubKey = PubKeyScript(
                asm = "OP_DUP OP_HASH160 ea132286328cfc819457b9dec386c4b5c84faa5c OP_EQUALVERIFY OP_CHECKSIG",
                hex = "76a914ea132286328cfc819457b9dec386c4b5c84faa5c88ac",
                reqSigs = 1, type = "pubkeyhash", addresses = listOf("1NLg3QJMsMQGM5KEUaEu5ADDmKQSLHwmyh")
        )
)

private val transaction = Transaction(
        hex = "01000000010000000000000000000000000000000000000000000000000000000000000000f...",
        txid = "90743aad855880e517270550d2a881627d84db5265142fd1e7fb7add38b08be9",
        version = 1, locktime = 0,
        vin = listOf(regularVin, coinbaseVin),
        vout = listOf(regularOut)
)

@DisplayName("Btcd transaction deserialization test: ")
class TransactionDeserializerTest {

    @Test
    @DisplayName("Should parse transaction")
    fun testParseTransaction() {

        val deserializer = ObjectMapper().registerKotlinModule()
        deserializer.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        val parsedTransaction = deserializer.readValue(rawTransaction, Transaction::class.java)

        Assertions.assertEquals(transaction, parsedTransaction)
    }
}