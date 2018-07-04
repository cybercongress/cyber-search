package fund.cyber.pump.ethereum.client

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.stub
import fund.cyber.pump.ethereum.client.genesis.EthereumGenesisDataProvider
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumTx
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.reactivex.subscribers.TestSubscriber
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.web3j.protocol.Web3jService
import org.web3j.protocol.core.methods.response.EthBlock
import org.web3j.protocol.core.methods.response.EthBlockNumber
import org.web3j.protocol.core.methods.response.EthGetTransactionReceipt
import org.web3j.protocol.parity.JsonRpc2_0Parity
import org.web3j.protocol.parity.Parity
import org.web3j.protocol.parity.methods.response.ParityTracesResponse
import rx.Observable
import java.util.concurrent.CompletableFuture

@Suppress("LongMethod")
class EthereumBlockchainInterfaceTest {


    private val testData = ParityTestData()
    private val chainInfo = ChainInfo(ChainFamily.ETHEREUM)
    private val converter = ParityToEthereumBundleConverter(chainInfo)

    @Test
    fun blockBundleByNumberTest() {

        val blockNumber = 1L
        val ethBlock = testData.getBlock(blockNumber)
        val uncle = testData.getUncle(ethBlock.block.hash, 0)
        val receipt = testData.getReceipt("0xd7b10b163b1de8f8967d824ea73d996c476588a91a4c714ad897b135cf7fa4c5")
        val traces = testData.getTraces(blockNumber)


        val web3jServiceMock = mock<Web3jService>()

        web3jServiceMock.stub {
            on { send(any(), eq(EthBlock::class.java)) }.thenReturn(ethBlock)
            on { sendAsync(any(), eq(EthBlock::class.java)) }.thenReturn(CompletableFuture.completedFuture(uncle))
            on { sendAsync(any(), eq(EthGetTransactionReceipt::class.java)) }.thenReturn(CompletableFuture.completedFuture(receipt))
            on { send(any(), eq(ParityTracesResponse::class.java)) }.thenReturn(traces)
        }

        val jsonRpcParity = JsonRpc2_0Parity(web3jServiceMock)

        val genesisMock = mock<EthereumGenesisDataProvider> { }


        val blockchainInterface = EthereumBlockchainInterface(jsonRpcParity, converter, genesisMock, SimpleMeterRegistry())
        val bundle = blockchainInterface.blockBundleByNumber(1)

        Assertions.assertThat(bundle.number).isEqualTo(1)
        Assertions.assertThat(bundle.hash).isEqualTo("B")
        Assertions.assertThat(bundle.parentHash).isEqualTo("A")
        Assertions.assertThat(bundle.txes).hasSize(1)
        Assertions.assertThat(bundle.uncles).hasSize(1)
        Assertions.assertThat(bundle.block).isNotNull()
        Assertions.assertThat(bundle.block.hash).isEqualTo("B")
        Assertions.assertThat(bundle.txes[0].hash).isEqualTo("d7b10b163b1de8f8967d824ea73d996c476588a91a4c714ad897b135cf7fa4c5")
        Assertions.assertThat(bundle.uncles[0].hash).isEqualTo("UB")
    }

    @Test
    fun subscribePoolTest() {

        val mempoolTx = testData.getTx("0xc713064951e38f55b167c483a882bd1550b12d58c552800e1e25a764e70a6894")

        val parityMock = mock<Parity> {
            on { pendingTransactionObservable() }.thenReturn(Observable.just(mempoolTx))
        }

        val genesisMock = mock<EthereumGenesisDataProvider> { }

        val blockchainInterface = EthereumBlockchainInterface(parityMock, converter, genesisMock, SimpleMeterRegistry())

        val mempoolFlowable = blockchainInterface.subscribePool()

        val testSubscriber = TestSubscriber<EthereumTx>()
        mempoolFlowable.subscribe(testSubscriber)

        testSubscriber.assertComplete()
        testSubscriber.assertNoErrors()
        testSubscriber.assertValueCount(1)
        Assertions.assertThat(testSubscriber.values()[0].hash).isEqualTo("c713064951e38f55b167c483a882bd1550b12d58c552800e1e25a764e70a6894")
    }

    @Test
    fun lastNetworkBlockTest() {

        val ethBlockNumber = EthBlockNumber()
        ethBlockNumber.result = "0x64"

        val web3jServiceMock = mock<Web3jService>()

        web3jServiceMock.stub {
            on { send(any(), eq(EthBlockNumber::class.java)) }.thenReturn(ethBlockNumber)
        }

        val jsonRpcParity = JsonRpc2_0Parity(web3jServiceMock)

        val genesisMock = mock<EthereumGenesisDataProvider> { }


        val blockchainInterface = EthereumBlockchainInterface(jsonRpcParity, converter, genesisMock, SimpleMeterRegistry())
        val lastNetworkBlock = blockchainInterface.lastNetworkBlock()

        Assertions.assertThat(lastNetworkBlock).isEqualTo(100)
    }
}

