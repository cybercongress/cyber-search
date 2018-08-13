package fund.cyber.pump.ethereum.client

import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.CallOperation
import fund.cyber.search.model.ethereum.CallOperationResult
import fund.cyber.search.model.ethereum.OperationTrace
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

@Suppress("LongMethod")
class ParityToEthereumBundleConverterTest {

    private val chainInfo = ChainInfo(ChainFamily.ETHEREUM)
    private val testData = ParityTestData()
    private val converter = ParityToEthereumBundleConverter(chainInfo)


    @Test
    fun convertTest() {
        val ethBlock = testData.getBlock(1)
        val uncle = testData.getUncle("0xB", 0)
        val receipt = testData.getReceipt("0xd7b10b163b1de8f8967d824ea73d996c476588a91a4c714ad897b135cf7fa4c5")
        val tracesResponse = testData.getTraces(1)


        val rawData = BundleRawData(ethBlock.block, listOf(uncle.block), listOf(receipt.result), tracesResponse.traces)

        val bundle = converter.convert(rawData)

        Assertions.assertThat(bundle.hash).isEqualTo("B")
        Assertions.assertThat(bundle.parentHash).isEqualTo("A")
        Assertions.assertThat(bundle.number).isEqualTo(1)
        Assertions.assertThat(bundle.blockSize).isEqualTo(101)

        Assertions.assertThat(bundle.block).isNotNull()
        Assertions.assertThat(bundle.block.hash).isEqualTo("B")
        Assertions.assertThat(bundle.block.number).isEqualTo(1)
        Assertions.assertThat(bundle.block.parentHash).isEqualTo("A")
        Assertions.assertThat(bundle.block.timestamp).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.block.sha3Uncles).isEqualTo("")
        Assertions.assertThat(bundle.block.logsBloom).isEqualTo("")
        Assertions.assertThat(bundle.block.transactionsRoot).isEqualTo("TXROOT")
        Assertions.assertThat(bundle.block.stateRoot).isEqualTo("")
        Assertions.assertThat(bundle.block.receiptsRoot).isEqualTo("")
        Assertions.assertThat(bundle.block.minerContractHash).isEqualTo("MINER")
        Assertions.assertThat(bundle.block.difficulty).isEqualTo(100)
        Assertions.assertThat(bundle.block.totalDifficulty).isEqualTo(100)
        Assertions.assertThat(bundle.block.extraData).isEqualTo("")
        Assertions.assertThat(bundle.block.size).isEqualTo(101)
        Assertions.assertThat(bundle.block.gasLimit).isEqualTo(200)
        Assertions.assertThat(bundle.block.gasUsed).isEqualTo(150)
        Assertions.assertThat(bundle.block.txNumber).isEqualTo(1)
        Assertions.assertThat(bundle.block.uncles.size).isEqualTo(1)
        Assertions.assertThat(bundle.block.blockReward.stripTrailingZeros()).isEqualTo(BigDecimal(5.15625))
        Assertions.assertThat(bundle.block.unclesReward.stripTrailingZeros()).isEqualTo(BigDecimal(7.5))
        Assertions.assertThat(bundle.block.txFees).isEqualTo(BigDecimal("0.000084000000000000"))

        Assertions.assertThat(bundle.uncles.size).isEqualTo(1)
        Assertions.assertThat(bundle.uncles[0].hash).isEqualTo("UB")
        Assertions.assertThat(bundle.uncles[0].position).isEqualTo(0)
        Assertions.assertThat(bundle.uncles[0].number).isEqualTo(1)
        Assertions.assertThat(bundle.uncles[0].timestamp).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.uncles[0].blockNumber).isEqualTo(1)
        Assertions.assertThat(bundle.uncles[0].blockTime).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.uncles[0].blockHash).isEqualTo("B")
        Assertions.assertThat(bundle.uncles[0].miner).isEqualTo("UMINER")
        Assertions.assertThat(bundle.uncles[0].uncleReward.stripTrailingZeros()).isEqualTo(BigDecimal(7.5))


        Assertions.assertThat(bundle.txes.size).isEqualTo(1)
        Assertions.assertThat(bundle.txes[0].hash).isEqualTo("d7b10b163b1de8f8967d824ea73d996c476588a91a4c714ad897b135cf7fa4c5")
        Assertions.assertThat(bundle.txes[0].error).isNull()
        Assertions.assertThat(bundle.txes[0].nonce).isEqualTo(0)
        Assertions.assertThat(bundle.txes[0].blockHash).isEqualTo("B")
        Assertions.assertThat(bundle.txes[0].blockNumber).isEqualTo(1)
        Assertions.assertThat(bundle.txes[0].firstSeenTime).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.txes[0].blockTime).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.txes[0].positionInBlock).isEqualTo(0)
        Assertions.assertThat(bundle.txes[0].from).isEqualTo("52bc44d5378309ee2abf1539bf71de1b7d7be3b5")
        Assertions.assertThat(bundle.txes[0].to).isEqualTo("00b654761a1b4372a77f6a327893572f55a483c7")
        Assertions.assertThat(bundle.txes[0].value).isEqualTo(BigDecimal("0.491554978522159616"))
        Assertions.assertThat(bundle.txes[0].gasPrice).isEqualTo(BigDecimal("0.000000004000000000"))
        Assertions.assertThat(bundle.txes[0].gasLimit).isEqualTo(50000)
        Assertions.assertThat(bundle.txes[0].gasUsed).isEqualTo(21000)
        Assertions.assertThat(bundle.txes[0].fee).isEqualTo(BigDecimal("0.000084000000000000"))
        Assertions.assertThat(bundle.txes[0].input).isEqualTo("0x")
        Assertions.assertThat(bundle.txes[0].createdSmartContract).isNull()

        val expectedOperation = CallOperation(
            type = "call", from = "52bc44d5378309ee2abf1539bf71de1b7d7be3b5", input = "0x", gasLimit = 50000,
            to = "00b654761a1b4372a77f6a327893572f55a483c7", value = BigDecimal("0.491554978522159616")
        )

        val expectedOperationResult = CallOperationResult(output = "0x", gasUsed = 21000)
        val expectedRootOperationTrace = OperationTrace(expectedOperation, expectedOperationResult, emptyList())

        Assertions.assertThat(bundle.txes[0].trace).isNotNull()
        Assertions.assertThat(bundle.txes[0].trace).isNotNull()
        Assertions.assertThat(bundle.txes[0].trace!!.rootOperationTrace).isNotNull()
        Assertions.assertThat(bundle.txes[0].trace!!.rootOperationTrace).isEqualTo(expectedRootOperationTrace)
    }


    @Test
    fun convertMempoolTxTest() {

        val mempoolTx = testData.getTx("0xc713064951e38f55b167c483a882bd1550b12d58c552800e1e25a764e70a6894")

        val convertedTx = converter.parityMempoolTxToDao(mempoolTx)
        Assertions.assertThat(convertedTx.hash).isEqualTo("c713064951e38f55b167c483a882bd1550b12d58c552800e1e25a764e70a6894")
        Assertions.assertThat(convertedTx.from).isEqualTo("eba74648d1eb1ccea85043f572b5e618dc28ced0")
        Assertions.assertThat(convertedTx.to).isEqualTo("1cb9abf92efd5a552afccea4fd5c0d5d45e593d9")
        Assertions.assertThat(convertedTx.blockHash).isNull()
        Assertions.assertThat(convertedTx.blockTime).isNull()
        Assertions.assertThat(convertedTx.positionInBlock).isEqualTo(-1)
        Assertions.assertThat(convertedTx.blockNumber).isEqualTo(-1)
        Assertions.assertThat(convertedTx.nonce).isEqualTo(0)
        Assertions.assertThat(convertedTx.error).isNull()
        Assertions.assertThat(convertedTx.firstSeenTime).isBeforeOrEqualTo(Instant.now())
        Assertions.assertThat(convertedTx.value).isEqualTo(BigDecimal("0.491554978522159616"))
        Assertions.assertThat(convertedTx.gasPrice).isEqualTo(BigDecimal("0.000000004000000000"))
        Assertions.assertThat(convertedTx.gasLimit).isEqualTo(50000)
        Assertions.assertThat(convertedTx.gasUsed).isEqualTo(0)
        Assertions.assertThat(convertedTx.fee).isEqualTo(BigDecimal("0.000200000000000000"))
        Assertions.assertThat(convertedTx.input).isEqualTo("0x")
        Assertions.assertThat(convertedTx.createdSmartContract).isNull()
        Assertions.assertThat(convertedTx.trace).isNull()
    }
}
