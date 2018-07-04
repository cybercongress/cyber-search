package fund.cyber.pump.bitcoin.client

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.pump.bitcoin.client.converter.BitcoinTxOutputsStorage
import fund.cyber.pump.bitcoin.client.converter.txOutputCacheKey
import fund.cyber.search.model.bitcoin.BitcoinCacheTxOutput
import fund.cyber.search.model.bitcoin.CoinbaseTransactionInput
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransaction
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransactionOutput
import fund.cyber.search.model.bitcoin.PubKeyScript
import fund.cyber.search.model.bitcoin.RegularTransactionInput
import fund.cyber.search.model.bitcoin.SignatureScript
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions
import org.ehcache.Cache
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class BitcoinTxOutputsStorageTest {


    @Test
    fun updateCacheTest() {

        val outNumber = 0
        val txid = "0xTXA"

        val rpcOutput = JsonRpcBitcoinTransactionOutput(
            value = BigDecimal("50"), n = outNumber,
            scriptPubKey = PubKeyScript(
                asm = "0xASM", hex = "0xHEX", reqSigs = 0, type = "C", addresses = listOf("0xMINER")
            )
        )

        val rpcBlock = rpcBlock(
            listOf(
                JsonRpcBitcoinTransaction(
                    txid = txid, hex = "0x123", version = 0, size = 300, locktime = 11111111,
                    vin = listOf(
                        CoinbaseTransactionInput(coinbase = "0xcoinbase", sequence = 0)
                    ),
                    vout = listOf(rpcOutput)
                )
            )
        )

        val rpcClientMock = mock<BitcoinJsonRpcClient> { }
        val cacheMock = mock<Cache<String, BitcoinCacheTxOutput>> { }

        val outputsStorage = BitcoinTxOutputsStorage(rpcClientMock, cacheMock, SimpleMeterRegistry())

        outputsStorage.updateCache(rpcBlock)

        verify(cacheMock, times(1)).put((txid to outNumber).txOutputCacheKey(), BitcoinCacheTxOutput(txid, rpcOutput))
    }

    @Test
    fun updateCacheNullCacheTest() {

        val outNumber = 0
        val txid = "0xTXA"

        val rpcOutput = JsonRpcBitcoinTransactionOutput(
            value = BigDecimal("50"), n = outNumber,
            scriptPubKey = PubKeyScript(
                asm = "0xASM", hex = "0xHEX", reqSigs = 0, type = "C", addresses = listOf("0xMINER")
            )
        )

        val rpcBlock = rpcBlock(
            listOf(
                JsonRpcBitcoinTransaction(
                    txid = txid, hex = "0x123", version = 0, size = 300, locktime = 11111111,
                    vin = listOf(
                        CoinbaseTransactionInput(coinbase = "0xcoinbase", sequence = 0)
                    ),
                    vout = listOf(rpcOutput)
                )
            )
        )

        val rpcClientMock = mock<BitcoinJsonRpcClient> { }

        val outputsStorage = BitcoinTxOutputsStorage(rpcClientMock, null, SimpleMeterRegistry())

        outputsStorage.updateCache(rpcBlock)
    }

    @Test
    fun getLinkedOutputsByBlockTest() {

        val rpcBlock = rpcBlock(
            listOf(
                rpcRegularTx("0xTXA", "0xTXB", "0xB", BigDecimal("42")),
                rpcRegularTx("0xTXC", "0xTXD", "0xC", BigDecimal("322"))
            )
        )

        val rpcClientMock = mock<BitcoinJsonRpcClient> {
            on { getTxes(setOf("0xTXD")) }.thenReturn(
                listOf(
                    rpcRegularTx("0xTXD", "0xTXD1", "0xD", BigDecimal("322"))
                )
            )
        }

        val cacheMock = mock<Cache<String, BitcoinCacheTxOutput>> {
            on { get(("0xTXB" to 0).txOutputCacheKey()) }.thenReturn(
                BitcoinCacheTxOutput(txid = "0xTXB", value = BigDecimal("43"), n = 0, addresses = listOf("0xA"))
            )
        }

        val outputsStorage = BitcoinTxOutputsStorage(rpcClientMock, cacheMock, SimpleMeterRegistry())

        val linkedOutputs = outputsStorage.getLinkedOutputsByBlock(rpcBlock)

        verify(cacheMock, times(1)).remove(("0xTXB" to 0).txOutputCacheKey())

        Assertions.assertThat(linkedOutputs).hasSize(2)

        Assertions.assertThat(linkedOutputs[0].txid).isEqualTo("0xTXB")
        Assertions.assertThat(linkedOutputs[0].addresses).hasSize(1)
        Assertions.assertThat(linkedOutputs[0].addresses[0]).isEqualTo("0xA")
        Assertions.assertThat(linkedOutputs[0].value).isEqualTo(BigDecimal("43"))
        Assertions.assertThat(linkedOutputs[0].n).isEqualTo(0)

        Assertions.assertThat(linkedOutputs[1].txid).isEqualTo("0xTXD")
        Assertions.assertThat(linkedOutputs[1].addresses).hasSize(1)
        Assertions.assertThat(linkedOutputs[1].addresses[0]).isEqualTo("0xD")
        Assertions.assertThat(linkedOutputs[1].value).isEqualTo(BigDecimal("322"))
        Assertions.assertThat(linkedOutputs[1].n).isEqualTo(0)
    }

    @Test
    fun getLinkedOutputsByBlockWithoutCacheTest() {

        val rpcBlock = rpcBlock(
            listOf(
                rpcRegularTx("0xTXA", "0xTXB", "0xB", BigDecimal("42")),
                rpcRegularTx("0xTXC", "0xTXD", "0xC", BigDecimal("322"))
            )
        )

        val rpcClientMock = mock<BitcoinJsonRpcClient> {
            on { getTxes(setOf("0xTXB", "0xTXD")) }.thenReturn(
                listOf(
                    rpcRegularTx("0xTXB", "0xTXB1", "0xA", BigDecimal("43")),
                    rpcRegularTx("0xTXD", "0xTXD1", "0xD", BigDecimal("322"))
                )
            )
        }


        val outputsStorage = BitcoinTxOutputsStorage(rpcClientMock, null, SimpleMeterRegistry())

        val linkedOutputs = outputsStorage.getLinkedOutputsByBlock(rpcBlock)


        Assertions.assertThat(linkedOutputs).hasSize(2)

        Assertions.assertThat(linkedOutputs[0].txid).isEqualTo("0xTXB")
        Assertions.assertThat(linkedOutputs[0].addresses).hasSize(1)
        Assertions.assertThat(linkedOutputs[0].addresses[0]).isEqualTo("0xA")
        Assertions.assertThat(linkedOutputs[0].value).isEqualTo(BigDecimal("43"))
        Assertions.assertThat(linkedOutputs[0].n).isEqualTo(0)

        Assertions.assertThat(linkedOutputs[1].txid).isEqualTo("0xTXD")
        Assertions.assertThat(linkedOutputs[1].addresses).hasSize(1)
        Assertions.assertThat(linkedOutputs[1].addresses[0]).isEqualTo("0xD")
        Assertions.assertThat(linkedOutputs[1].value).isEqualTo(BigDecimal("322"))
        Assertions.assertThat(linkedOutputs[1].n).isEqualTo(0)
    }

    @Test
    fun getLinkedOutputsByTxTest() {

        val rpcTx = rpcRegularTx("0xTXA", "0xTXB", "0xB", BigDecimal("42"))

        val rpcClientMock = mock<BitcoinJsonRpcClient> { }

        val cacheMock = mock<Cache<String, BitcoinCacheTxOutput>> {
            on { get(("0xTXB" to 0).txOutputCacheKey()) }.thenReturn(
                BitcoinCacheTxOutput(txid = "0xTXB", value = BigDecimal("43"), n = 0, addresses = listOf("0xA"))
            )
        }

        val outputsStorage = BitcoinTxOutputsStorage(rpcClientMock, cacheMock, SimpleMeterRegistry())

        val linkedOutputs = outputsStorage.getLinkedOutputsByTx(rpcTx)

        verify(cacheMock, times(1)).remove(("0xTXB" to 0).txOutputCacheKey())

        Assertions.assertThat(linkedOutputs).hasSize(1)

        Assertions.assertThat(linkedOutputs[0].txid).isEqualTo("0xTXB")
        Assertions.assertThat(linkedOutputs[0].addresses).hasSize(1)
        Assertions.assertThat(linkedOutputs[0].addresses[0]).isEqualTo("0xA")
        Assertions.assertThat(linkedOutputs[0].value).isEqualTo(BigDecimal("43"))
        Assertions.assertThat(linkedOutputs[0].n).isEqualTo(0)
    }

    fun rpcRegularTx(txid: String, fromTxId: String, to: String, value: BigDecimal) = JsonRpcBitcoinTransaction(
        txid = txid, hex = "0x123", version = 0, size = 300, locktime = 11111111,
        vin = listOf(
            RegularTransactionInput(
                txid = fromTxId, vout = 0, scriptSig = SignatureScript("0xASM", "0xHEX"), sequence = 0
            )
        ),
        vout = listOf(
            JsonRpcBitcoinTransactionOutput(
                value = value, n = 0,
                scriptPubKey = PubKeyScript(
                    asm = "0xASM", hex = "0xHEX", reqSigs = 0, type = "C", addresses = listOf(to)
                )
            )
        )
    )

    fun rpcBlock(transactions: List<JsonRpcBitcoinTransaction>) = JsonRpcBitcoinBlock(
        hash = "0xB", confirmations = 1, strippedsize = 100, size = 101,
        height = 1, weight = 102, version = 2, merkleroot = "0xmerkleROOT", time = 11111111, nonce = 124,
        bits = "0x123", difficulty = BigDecimal("104"), previousblockhash = "0xA", nextblockhash = "0xC",
        tx = transactions
    )


}
