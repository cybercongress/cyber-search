package fund.cyber.contract.ethereum.delta

import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.txPumpTopic
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

class EthereumDeltaMergerTest {

    private val deltaMerger = EthereumDeltaMerger()
    private val chainInfo = ChainInfo(ChainFamily.ETHEREUM)

    @Test
    fun mergerTest() {
        val delta1 = delta("a", BigDecimal("1.1"), offset = 0)
        val delta2 = delta("a", BigDecimal("1.2"), offset = 2)
        val delta3 = delta("a", BigDecimal("1.3"), offset = 4)
        val delta4 = delta("a", BigDecimal("1.4"), offset = 6)
        val delta5 = delta("a", BigDecimal("1.5"), offset = 8)

        val currentContracts = mapOf(
            "a" to contractSummary("a", 2)
        )

        val mergedDelta = deltaMerger.mergeDeltas(listOf(delta1, delta2, delta3, delta4, delta5), currentContracts)

        Assertions.assertThat(mergedDelta).isNotNull()
        Assertions.assertThat(mergedDelta!!).isEqualTo(
            EthereumContractSummaryDelta(
                contract = "a", smartContract = false,
                balanceDelta = BigDecimal("4.2"), totalReceivedDelta = BigDecimal("4.2"),
                txNumberDelta = 0,
                successfulOpNumberDelta = 3,
                topic = chainInfo.txPumpTopic, partition = 0, offset = 8, lastOpTime = Instant.ofEpochMilli(100000)
            )
        )

    }

    @Test
    fun mergerWithNullTest() {
        val delta1 = delta("a", BigDecimal("1.1"), offset = 0)
        val delta2 = delta("a", BigDecimal("1.2"), offset = 2)
        val delta3 = delta("a", BigDecimal("1.3"), offset = 4)
        val delta4 = delta("a", BigDecimal("1.4"), offset = 6)
        val delta5 = delta("a", BigDecimal("1.5"), offset = 8)

        val currentContracts = mapOf(
            "a" to contractSummary("a", 8)
        )

        val mergedDelta = deltaMerger.mergeDeltas(listOf(delta1, delta2, delta3, delta4, delta5), currentContracts)

        Assertions.assertThat(mergedDelta).isNull()
    }

}
