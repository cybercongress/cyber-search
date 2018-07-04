package fund.cyber.pump.bitcoin.client

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.pump.bitcoin.client.converter.BitcoinTxOutputsStorage
import fund.cyber.pump.bitcoin.client.converter.JsonRpcBlockToBitcoinBundleConverter
import fund.cyber.search.model.bitcoin.BitcoinCacheTxOutput
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.CoinbaseTransactionInput
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransaction
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransactionOutput
import fund.cyber.search.model.bitcoin.PubKeyScript
import fund.cyber.search.model.bitcoin.RegularTransactionInput
import fund.cyber.search.model.bitcoin.SignatureScript
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.reactivex.subscribers.TestSubscriber
import org.assertj.core.api.Assertions
import org.ehcache.Cache
import org.junit.jupiter.api.Test
import org.springframework.retry.support.RetryTemplate
import java.math.BigDecimal

class BitcoinBlockchainInterfaceTest {


    @Test
    fun lastNetworkBlockTest() {

        val rpcClientMock = mock<BitcoinJsonRpcClient> {
            on { getLastBlockNumber() }.thenReturn(100)
        }
        val converter = JsonRpcBlockToBitcoinBundleConverter(
            BitcoinTxOutputsStorage(rpcClientMock, null, SimpleMeterRegistry())
        )
        val mempoolHashesCacheMock = mock<Cache<String, String>> { }
        val blockchainInterface = BitcoinBlockchainInterface(
            rpcClientMock, converter, SimpleMeterRegistry(), mempoolHashesCacheMock, RetryTemplate()
        )

        val lastNetworkBlock = blockchainInterface.lastNetworkBlock()
        Assertions.assertThat(lastNetworkBlock).isEqualTo(100)
    }


    @Test
    fun blockBundleByNumberTest() {

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

        val rpcClientMock = mock<BitcoinJsonRpcClient> {
            on { getBlockByNumber(1) }.thenReturn(rpcBlock)
        }
        val outputsStorageMock = mock<BitcoinTxOutputsStorage> {
            on { getLinkedOutputsByBlock(any()) }.thenReturn(emptyList())
        }
        val converter = JsonRpcBlockToBitcoinBundleConverter(outputsStorageMock)
        val mempoolHashesCacheMock = mock<Cache<String, String>> { }


        val blockchainInterface = BitcoinBlockchainInterface(
            rpcClientMock, converter, SimpleMeterRegistry(), mempoolHashesCacheMock, RetryTemplate()
        )

        val bundle = blockchainInterface.blockBundleByNumber(1)

        Assertions.assertThat(bundle.hash).isEqualTo("B")
        Assertions.assertThat(bundle.parentHash).isEqualTo("A")
        Assertions.assertThat(bundle.number).isEqualTo(1)
        Assertions.assertThat(bundle.block.hash).isEqualTo("B")
        Assertions.assertThat(bundle.block.parentHash).isEqualTo("A")
        Assertions.assertThat(bundle.block.number).isEqualTo(1)
        Assertions.assertThat(bundle.transactions).hasSize(1)
        Assertions.assertThat(bundle.transactions[0].hash).isEqualTo("TXA")
    }

    @Test
    fun subscribePoolTest() {

        val mempoolTx = JsonRpcBitcoinTransaction(
            txid = "0xTXA", hex = "0x123", version = 0, size = 300, locktime = 11111111,
            vin = listOf(
                RegularTransactionInput(
                    txid = "0xTXB", vout = 0, scriptSig = SignatureScript("0xASM", "0xHEX"), sequence = 0
                )
            ),
            vout = listOf(
                JsonRpcBitcoinTransactionOutput(
                    value = BigDecimal("42"), n = 0,
                    scriptPubKey = PubKeyScript(
                        asm = "0xASM", hex = "0xHEX", reqSigs = 0, type = "C", addresses = listOf("0xB")
                    )
                )
            )
        )

        val rpcClientMock = mock<BitcoinJsonRpcClient> {
            on { getTxMempool() }.thenReturn(listOf("0xTXA1")).thenReturn(listOf("0xTXA"))
            on { getTx("0xTXA") }.thenReturn(mempoolTx)
        }

        val outputsStorageMock = mock<BitcoinTxOutputsStorage> {
            on { getLinkedOutputsByTx(mempoolTx) }.thenReturn(
                listOf(
                    BitcoinCacheTxOutput(txid = "0xTXB", value = BigDecimal("43"), n = 0, addresses = listOf("A"))
                )
            )
        }

        val converter = JsonRpcBlockToBitcoinBundleConverter(outputsStorageMock)
        val mempoolHashesCacheMock = mock<Cache<String, String>> { }


        val blockchainInterface = BitcoinBlockchainInterface(
            rpcClientMock, converter, SimpleMeterRegistry(), mempoolHashesCacheMock, RetryTemplate()
        )

        val poolFlowable = blockchainInterface.subscribePool()

        val testSubscriber = TestSubscriber<BitcoinTx>()

        poolFlowable.subscribe(testSubscriber)
        testSubscriber.awaitCount(1)
        Assertions.assertThat(testSubscriber.values()[0].hash).isEqualTo("TXA")


        verify(mempoolHashesCacheMock, times(1)).put("0xTXA1", "")
        verify(mempoolHashesCacheMock, times(1)).put("0xTXA", "")
    }
}
