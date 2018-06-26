package fund.cyber.contract.bitcoin.delta

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class BitcoinContractSummaryDeltaTest {


    @Test
    fun methodsTest() {
        val delta1 = delta("a", BigDecimal.ONE, txNumberDelta = 1)
        val summary1 = CqlBitcoinContractSummary(
            hash = delta1.contract, confirmedBalance = delta1.balanceDelta.toString(),
            confirmedTxNumber = delta1.txNumberDelta,
            confirmedTotalReceived = delta1.totalReceivedDelta.toString(),
            firstActivityDate = delta1.time, lastActivityDate = delta1.time,
            kafkaDeltaOffset = delta1.offset, kafkaDeltaTopic = delta1.topic,
            kafkaDeltaPartition = delta1.partition, version = 0
        )

        Assertions.assertThat(delta1.createSummary()).isEqualTo(summary1)

        val delta2 = delta("a", BigDecimal.ONE, txNumberDelta = 1)
        val summary2 = delta2.updateSummary(summary1)

        Assertions.assertThat(summary2).isEqualTo(
            CqlBitcoinContractSummary(
                hash = summary1.hash,
                confirmedBalance = (BigDecimal(summary1.confirmedBalance) + delta2.balanceDelta).toString(),
                confirmedTxNumber = summary1.confirmedTxNumber + delta2.txNumberDelta,
                firstActivityDate = summary1.firstActivityDate, lastActivityDate = delta2.time,
                confirmedTotalReceived = (BigDecimal(summary1.confirmedTotalReceived) + delta2.totalReceivedDelta)
                    .toString(),
                kafkaDeltaOffset = delta2.offset, kafkaDeltaTopic = delta2.topic,
                kafkaDeltaPartition = delta2.partition, version = summary1.version + 1
            )
        )
    }

}
