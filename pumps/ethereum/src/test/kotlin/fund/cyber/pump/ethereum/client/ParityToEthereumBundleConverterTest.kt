package fund.cyber.pump.ethereum.client

import fund.cyber.common.DECIMAL_SCALE
import fund.cyber.common.decimal32
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.CallOperation
import fund.cyber.search.model.ethereum.CallOperationResult
import fund.cyber.search.model.ethereum.OperationTrace
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant

@Suppress("LongMethod")
class ParityToEthereumBundleConverterTest {

    private val chainInfo = ChainInfo(ChainFamily.ETHEREUM)
    private val testData = ParityTestData()
    private val converter = ParityToEthereumBundleConverter(chainInfo)

    @Test
    fun convertTwoUnclesDifferentMinerTest() {
        val ethBlock = testData.getBlock(6149845)
        val uncle0 = testData.getUncle("0x02e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7", 0)
        val uncle1 = testData.getUncle("0x02e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7", 1)
        val receipt = testData.getReceipt("0xd7b10b163b1de8f8967d824ea73d996c476588a91a4c714ad897b135cf7fa4c5")
        val tracesResponse = testData.getTraces(6149845)

        val rawData = BundleRawData(ethBlock.block, listOf(uncle0.block, uncle1.block), listOf(receipt.result), tracesResponse.traces)

        val bundle = converter.convert(rawData)

        Assertions.assertThat(bundle.hash).isEqualTo("02e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7")
        Assertions.assertThat(bundle.parentHash).isEqualTo("A")
        Assertions.assertThat(bundle.number).isEqualTo(6149845)
        Assertions.assertThat(bundle.blockSize).isEqualTo(101)

        Assertions.assertThat(bundle.block).isNotNull()
        Assertions.assertThat(bundle.block.hash).isEqualTo("02e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7")
        Assertions.assertThat(bundle.block.number).isEqualTo(6149845)
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
        Assertions.assertThat(bundle.block.uncles.size).isEqualTo(2)
        val expectedIncludingUnclesReward = ((3 * 2).toBigDecimal()).divide(decimal32, DECIMAL_SCALE, RoundingMode.FLOOR).stripTrailingZeros()
        Assertions.assertThat(bundle.block.unclesReward.stripTrailingZeros()).isEqualTo(expectedIncludingUnclesReward)
        Assertions.assertThat(bundle.block.blockReward.stripTrailingZeros()).isEqualTo(BigDecimal(3))
        Assertions.assertThat(bundle.block.txFees).isEqualTo(BigDecimal("0.000084000000000000"))

        Assertions.assertThat(bundle.uncles.size).isEqualTo(2)
        Assertions.assertThat(bundle.uncles[0].hash).isEqualTo("772205e8de3b9d52bc6c410c7adf1348abb32c97adbee6aebdd9a2bb33f7fbf8")
        Assertions.assertThat(bundle.uncles[0].position).isEqualTo(0)
        Assertions.assertThat(bundle.uncles[0].number).isEqualTo(6149845)
        Assertions.assertThat(bundle.uncles[0].timestamp).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.uncles[0].blockNumber).isEqualTo(6149845)
        Assertions.assertThat(bundle.uncles[0].blockTime).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.uncles[0].blockHash).isEqualTo("02e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7")
        Assertions.assertThat(bundle.uncles[0].miner).isEqualTo("c8ebccc5f5689fa8659d83713341e5ad19349448")
        Assertions.assertThat(bundle.uncles[0].uncleReward.stripTrailingZeros()).isEqualTo(BigDecimal(2.625))
        Assertions.assertThat(bundle.uncles[1].hash).isEqualTo("a435f091388391e8edec523a26c127776c6d469dee9f8812aaade52f0f31f402")
        Assertions.assertThat(bundle.uncles[1].position).isEqualTo(1)
        Assertions.assertThat(bundle.uncles[1].number).isEqualTo(6149845)
        Assertions.assertThat(bundle.uncles[1].timestamp).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.uncles[1].blockNumber).isEqualTo(6149845)
        Assertions.assertThat(bundle.uncles[1].blockTime).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.uncles[1].blockHash).isEqualTo("02e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7")
        Assertions.assertThat(bundle.uncles[1].miner).isEqualTo("c8ebccc5f5689fa8659d83713341e5ad19349441")
        Assertions.assertThat(bundle.uncles[1].uncleReward.stripTrailingZeros()).isEqualTo(BigDecimal(2.25))


        Assertions.assertThat(bundle.txes.size).isEqualTo(1)
        Assertions.assertThat(bundle.txes[0].hash).isEqualTo("d7b10b163b1de8f8967d824ea73d996c476588a91a4c714ad897b135cf7fa4c5")
        Assertions.assertThat(bundle.txes[0].error).isNull()
        Assertions.assertThat(bundle.txes[0].nonce).isEqualTo(0)
        Assertions.assertThat(bundle.txes[0].blockHash).isEqualTo("02e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7")
        Assertions.assertThat(bundle.txes[0].blockNumber).isEqualTo(6149845)
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
    fun convertTwoUnclesSameMinerTest() {
        val ethBlock = testData.getBlock(6154914)
        val uncle0 = testData.getUncle("0x12e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7", 0)
        val uncle1 = testData.getUncle("0x12e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7", 1)
        val tracesResponse = testData.getTraces(6154914)

        val rawData = BundleRawData(ethBlock.block, listOf(uncle0.block, uncle1.block), listOf(), tracesResponse.traces)

        val bundle = converter.convert(rawData)

        Assertions.assertThat(bundle.hash).isEqualTo("12e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7")
        Assertions.assertThat(bundle.parentHash).isEqualTo("A")
        Assertions.assertThat(bundle.number).isEqualTo(6154914)
        Assertions.assertThat(bundle.blockSize).isEqualTo(101)

        Assertions.assertThat(bundle.block).isNotNull()
        Assertions.assertThat(bundle.block.hash).isEqualTo("12e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7")
        Assertions.assertThat(bundle.block.number).isEqualTo(6154914)
        Assertions.assertThat(bundle.block.uncles.size).isEqualTo(2)
        val expectedUncleInclusionReward = ((3 * 2).toBigDecimal()).divide(decimal32, DECIMAL_SCALE, RoundingMode.FLOOR).stripTrailingZeros()
        Assertions.assertThat(bundle.block.unclesReward.stripTrailingZeros()).isEqualTo(expectedUncleInclusionReward)
        Assertions.assertThat(bundle.block.blockReward.stripTrailingZeros()).isEqualTo(BigDecimal(3))
        Assertions.assertThat(bundle.block.txFees).isEqualTo(BigDecimal("0"))

        Assertions.assertThat(bundle.uncles.size).isEqualTo(2)
        Assertions.assertThat(bundle.uncles[0].hash).isEqualTo("96aea1634772681458dafeedc968e05df41cf1da0f0ef38e548044ed1841e730")
        Assertions.assertThat(bundle.uncles[0].position).isEqualTo(0)
        Assertions.assertThat(bundle.uncles[0].number).isEqualTo(6154914)
        Assertions.assertThat(bundle.uncles[0].timestamp).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.uncles[0].blockNumber).isEqualTo(6154914)
        Assertions.assertThat(bundle.uncles[0].blockTime).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.uncles[0].blockHash).isEqualTo("12e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7")
        Assertions.assertThat(bundle.uncles[0].miner).isEqualTo("123")
        Assertions.assertThat(bundle.uncles[0].uncleReward.stripTrailingZeros()).isEqualTo(BigDecimal(2.625))
        Assertions.assertThat(bundle.uncles[1].hash).isEqualTo("527fa10104dc069db366e6e054251fd986c7ea3a49afebb539aa6f0b991baedc")
        Assertions.assertThat(bundle.uncles[1].position).isEqualTo(1)
        Assertions.assertThat(bundle.uncles[1].number).isEqualTo(6154914)
        Assertions.assertThat(bundle.uncles[1].timestamp).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.uncles[1].blockNumber).isEqualTo(6154914)
        Assertions.assertThat(bundle.uncles[1].blockTime).isEqualTo(Instant.ofEpochSecond(111111111))
        Assertions.assertThat(bundle.uncles[1].blockHash).isEqualTo("12e2ee54556a80710af3f80691781bf4eae37ae869d40bcf46b4186aeb6ce4d7")
        Assertions.assertThat(bundle.uncles[1].miner).isEqualTo("123")
        Assertions.assertThat(bundle.uncles[1].uncleReward.stripTrailingZeros()).isEqualTo(BigDecimal(1.875))
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
