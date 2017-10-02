package fund.cyber.index.btcd

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal

private val rawBlock = """
{
  "hash"             : "blockhash",
  "confirmations"    : 5,
  "strippedsize"     : 10,
  "size"             : 5,
  "weight"           : 322,
  "height"           : 322,
  "version"          : 1,
  "merkleroot"       : "1qwefsadf",
  "time"             : 35346574567,
  "nonce"            : 123123,
  "bits"             : "234234234",
  "difficulty"       : 2.33,
  "previousblockhash": "txid",
  "nextblockhash"    : "txid",
  "rawtx"               : [
    {
      "hex"     : "01000000010000000000000000000000000000000000000000000000000000000000000000f...",
      "txid"    : "90743aad855880e517270550d2a881627d84db5265142fd1e7fb7add38b08be9",
      "version" : 1,
      "size"    : 259,
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
  ]
}
"""

private val regularVin = BtcdRegularTransactionInput(
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

private val regularOut = BtcdTransactionOutput(
        value = "25.1394", n = 0,
        scriptPubKey = PubKeyScript(
                asm = "OP_DUP OP_HASH160 ea132286328cfc819457b9dec386c4b5c84faa5c OP_EQUALVERIFY OP_CHECKSIG",
                hex = "76a914ea132286328cfc819457b9dec386c4b5c84faa5c88ac",
                reqSigs = 1, type = "pubkeyhash", addresses = listOf("1NLg3QJMsMQGM5KEUaEu5ADDmKQSLHwmyh")
        )
)

private val transaction = BtcdTransaction(
        hex = "01000000010000000000000000000000000000000000000000000000000000000000000000f...",
        txid = "90743aad855880e517270550d2a881627d84db5265142fd1e7fb7add38b08be9",
        version = 1, locktime = 0, size = 259,
        vin = listOf(regularVin, coinbaseVin),
        vout = listOf(regularOut)
)

private val block = BtcdBlock(
        hash = "blockhash", confirmations = 5, size = 5, height = 322, strippedsize = 10,
        merkleroot = "1qwefsadf", time = 35346574567, nonce = 123123, difficulty = BigDecimal("2.33"),
        previousblockhash = "txid", nextblockhash = "txid", bits = "234234234", version = 1, weight = 322,
        rawtx = listOf(transaction)
)


@DisplayName("Btcd block deserialization test: ")
class BtcdBlockDeserializerTest {

    @Test
    @DisplayName("Should parse block")
    fun testParseBlock() {

        val deserializer = ObjectMapper().registerKotlinModule()
        deserializer.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        val parsedBlock = deserializer.readValue(rawBlock, BtcdBlock::class.java)

        Assertions.assertEquals(block, parsedBlock)
    }
}