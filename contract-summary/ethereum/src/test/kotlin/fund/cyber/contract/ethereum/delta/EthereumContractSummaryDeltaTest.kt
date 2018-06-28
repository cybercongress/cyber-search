package fund.cyber.contract.ethereum.delta

import fund.cyber.cassandra.ethereum.model.CqlEthereumContractSummary
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class EthereumContractSummaryDeltaTest {

    @Test
    fun methodsTest() {
        val delta1 = delta("a", BigDecimal.ONE)
        val summary1 = CqlEthereumContractSummary(
            hash = delta1.contract, confirmedBalance = delta1.balanceDelta.toString(),
            smartContract = delta1.smartContract ?: false,
            confirmedTotalReceived = delta1.totalReceivedDelta.toString(), txNumber = delta1.txNumberDelta,
            minedUncleNumber = delta1.uncleNumberDelta, minedBlockNumber = delta1.minedBlockNumberDelta,
            kafkaDeltaOffset = delta1.offset, kafkaDeltaTopic = delta1.topic,
            kafkaDeltaPartition = delta1.partition, version = 0, successfulOpNumber = delta1.successfulOpNumberDelta,
            firstActivityDate = delta1.lastOpTime, lastActivityDate = delta1.lastOpTime
        )

        Assertions.assertThat(delta1.createSummary()).isEqualTo(summary1)

        val delta2 = delta("a", BigDecimal.ONE)
        val summary2 = delta2.updateSummary(summary1)

        Assertions.assertThat(summary2).isEqualTo(
            CqlEthereumContractSummary(
                hash = summary1.hash,
                confirmedBalance = (BigDecimal(summary1.confirmedBalance) + delta2.balanceDelta).toString(),
                smartContract = summary1.smartContract,
                confirmedTotalReceived = (BigDecimal(summary1.confirmedTotalReceived) + delta2.totalReceivedDelta)
                    .toString(),
                txNumber = summary1.txNumber + delta2.txNumberDelta,
                successfulOpNumber = summary1.successfulOpNumber + delta2.successfulOpNumberDelta,
                minedUncleNumber = summary1.minedUncleNumber + delta2.uncleNumberDelta,
                minedBlockNumber = summary1.minedBlockNumber + delta2.minedBlockNumberDelta,
                kafkaDeltaOffset = delta2.offset, kafkaDeltaTopic = delta2.topic,
                kafkaDeltaPartition = delta2.partition, version = summary1.version + 1,
                firstActivityDate = summary1.firstActivityDate, lastActivityDate = delta2.lastOpTime
            )
        )
    }

}