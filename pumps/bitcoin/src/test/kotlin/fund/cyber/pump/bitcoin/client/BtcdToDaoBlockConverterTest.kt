package fund.cyber.pump.bitcoin.client

import fund.cyber.pump.bitcoin.client.converter.JsonRpcToDaoBitcoinBlockConverter
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.BitcoinTxOut
import fund.cyber.search.model.bitcoin.CoinbaseTransactionInput
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransaction
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransactionOutput
import fund.cyber.search.model.bitcoin.PubKeyScript
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

@Suppress("LongMethod")
class BtcdToDaoBlockConverterTest {

    @Test
    fun convertTest() {

        val converter = JsonRpcToDaoBitcoinBlockConverter()

        val rpcBlock = JsonRpcBitcoinBlock(
            hash = "0xB", confirmations = 1, strippedsize = 100, size = 101,
            height = 1, weight = 102, version = 2, merkleroot = "0xmerkleROOT", time = 11111111, nonce = 124,
            bits = "0x123", difficulty = BigDecimal("104"), previousblockhash = "0xA", nextblockhash = "0xC",
            tx = listOf(
                JsonRpcBitcoinTransaction(
                    txid = "0xTXA", hex = "0x123", version = 0, size = 300, locktime = 11111111,
                    vin = listOf(
                        CoinbaseTransactionInput(coinbase = "0xcoinbase", sequence = 0)
                    ),
                    vout = listOf(
                        JsonRpcBitcoinTransactionOutput(
                            value = BigDecimal("50"), n = 0,
                            scriptPubKey = PubKeyScript(
                                asm = "0xASM", hex = "0xHEX", reqSigs = 0, type = "C", addresses = listOf("0xMINER")
                            )
                        )
                    )
                )
            )
        )

        val transactions = listOf(
            BitcoinTx(
                hash = "0xTXA", blockNumber = 1, blockHash = "0xB", index = 0, coinbase = "0xcoinbase",
                firstSeenTime = Instant.ofEpochMilli(11111112), blockTime = Instant.ofEpochMilli(11111111),
                size = 300, fee = BigDecimal.ZERO, totalInputsAmount = BigDecimal("0"), totalOutputsAmount = BigDecimal("50"),
                ins = emptyList(),
                outs = listOf(
                    BitcoinTxOut(
                        contracts = listOf("0xMINER"), asm = "0xASM", out = 0, amount = BigDecimal("50"),
                        requiredSignatures = 1
                    )
                )
            )
        )

        val bitcoinBlock = converter.convertToDaoBlock(rpcBlock, transactions)

        Assertions.assertThat(bitcoinBlock.hash).isEqualTo("B")
        Assertions.assertThat(bitcoinBlock.parentHash).isEqualTo("A")
        Assertions.assertThat(bitcoinBlock.size).isEqualTo(101)
        Assertions.assertThat(bitcoinBlock.minerContractHash).isEqualTo("MINER")
        Assertions.assertThat(bitcoinBlock.version).isEqualTo(2)
        Assertions.assertThat(bitcoinBlock.blockReward).isEqualTo(BigDecimal("50"))
        Assertions.assertThat(bitcoinBlock.txFees).isEqualTo(BigDecimal.ZERO)
        Assertions.assertThat(bitcoinBlock.coinbaseData).isEqualTo("0xcoinbase")
        Assertions.assertThat(bitcoinBlock.bits).isEqualTo("0x123")
        Assertions.assertThat(bitcoinBlock.difficulty).isEqualTo(BigInteger("104"))
        Assertions.assertThat(bitcoinBlock.nonce).isEqualTo(124)
        Assertions.assertThat(bitcoinBlock.time).isEqualTo(Instant.ofEpochSecond(11111111))
        Assertions.assertThat(bitcoinBlock.weight).isEqualTo(102)
        Assertions.assertThat(bitcoinBlock.merkleroot).isEqualTo("merkleROOT")
        Assertions.assertThat(bitcoinBlock.height).isEqualTo(1)
        Assertions.assertThat(bitcoinBlock.txNumber).isEqualTo(1)
        Assertions.assertThat(bitcoinBlock.totalOutputsAmount).isEqualTo(BigDecimal("50"))

    }

    @Test
    fun convertWithoutCoinbaseAndWithoutParentTest() {

        val converter = JsonRpcToDaoBitcoinBlockConverter()

        val rpcBlock = JsonRpcBitcoinBlock(
            hash = "0xA", confirmations = 1, strippedsize = 100, size = 101,
            height = 1, weight = 102, version = 2, merkleroot = "0xmerkleROOT", time = 11111111, nonce = 124,
            bits = "0x123", difficulty = BigDecimal("104"), nextblockhash = "0xB", previousblockhash = null
        )

        val bitcoinBlock = converter.convertToDaoBlock(rpcBlock, emptyList())

        Assertions.assertThat(bitcoinBlock.hash).isEqualTo("A")
        Assertions.assertThat(bitcoinBlock.parentHash).isEqualTo("")
        Assertions.assertThat(bitcoinBlock.size).isEqualTo(101)
        Assertions.assertThat(bitcoinBlock.minerContractHash).isEqualTo("")
        Assertions.assertThat(bitcoinBlock.version).isEqualTo(2)
        Assertions.assertThat(bitcoinBlock.blockReward).isEqualTo(BigDecimal("50"))
        Assertions.assertThat(bitcoinBlock.txFees).isEqualTo(BigDecimal.ZERO)
        Assertions.assertThat(bitcoinBlock.coinbaseData).isEqualTo("")
        Assertions.assertThat(bitcoinBlock.bits).isEqualTo("0x123")
        Assertions.assertThat(bitcoinBlock.difficulty).isEqualTo(BigInteger("104"))
        Assertions.assertThat(bitcoinBlock.nonce).isEqualTo(124)
        Assertions.assertThat(bitcoinBlock.time).isEqualTo(Instant.ofEpochSecond(11111111))
        Assertions.assertThat(bitcoinBlock.weight).isEqualTo(102)
        Assertions.assertThat(bitcoinBlock.merkleroot).isEqualTo("merkleROOT")
        Assertions.assertThat(bitcoinBlock.height).isEqualTo(1)
        Assertions.assertThat(bitcoinBlock.txNumber).isEqualTo(0)
        Assertions.assertThat(bitcoinBlock.totalOutputsAmount).isEqualTo(BigDecimal("0"))

    }


}