package fund.cyber.contract.delta.apply

import com.nhaarman.mockito_kotlin.inOrder
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.cassandra.common.CqlContractSummary
import fund.cyber.contract.common.delta.ContractSummaryDelta
import fund.cyber.contract.common.delta.DeltaMerger
import fund.cyber.contract.common.delta.DeltaProcessor
import fund.cyber.contract.common.delta.apply.UpdateContractSummaryProcess
import fund.cyber.contract.common.summary.ContractSummaryStorage
import fund.cyber.search.model.events.PumpEvent
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal


data class TestDelta(
    override val contract: String,
    override val topic: String,
    override val partition: Int,
    override val offset: Long
) : ContractSummaryDelta<TestCqlContractSummary> {

    override fun createSummary(): TestCqlContractSummary {
        return TestCqlContractSummary(contract, 0, offset, partition, topic, false)
    }

    override fun updateSummary(summary: TestCqlContractSummary): TestCqlContractSummary {
        return TestCqlContractSummary(contract, 0, offset, partition, topic, false)
    }
}

data class TestCqlContractSummary(
    override val hash: String,
    override val version: Long,
    override val kafkaDeltaOffset: Long,
    override val kafkaDeltaPartition: Int,
    override val kafkaDeltaTopic: String,
    override val kafkaDeltaOffsetCommitted: Boolean
) : CqlContractSummary

data class TestTx(val contract: String, val value: BigDecimal)


// todo: Couldn't test everything because of SinglePartitionTopicLastItemsReader created right in code
class UpdateContractSummaryProcessTest {


    @Test
    fun normalFlowWithoutContractInDbTest() {
        val testTx = TestTx("a", BigDecimal.ONE)
        val testRecord = ConsumerRecord<PumpEvent, TestTx>("TEST_TX", 0, 0, PumpEvent.NEW_BLOCK, testTx)
        val recordsToProcess = listOf(testRecord)
        val testDelta = TestDelta("a", "TEST_TX", 0, 0)
        val testMergedDelta = TestDelta("a", "TEST_TX", 0, 0)


        val deltaProcessorMock = mock<DeltaProcessor<TestTx, TestCqlContractSummary, TestDelta>> {
            on { affectedContracts(recordsToProcess) }.thenReturn(setOf(testTx.contract))
            on { recordToDeltas(testRecord) }.thenReturn(listOf(testDelta))
        }
        val storageMock = mock<ContractSummaryStorage<TestCqlContractSummary>> {
            on { findAllByIdIn(setOf(testTx.contract)) }.thenReturn(Flux.empty())
            on { insertIfNotRecord(testMergedDelta.createSummary()) }.thenReturn(Mono.just(true))
            on { findById(testTx.contract) }.thenReturn(Mono.just(testDelta.createSummary()))
            on { commitUpdate(testTx.contract, 1) }.thenReturn(Mono.just(true))
        }

        val deltaMergerMock = mock<DeltaMerger<TestDelta>> {
            on { mergeDeltas(listOf(testDelta), emptyMap()) }.thenReturn(testMergedDelta)
        }

        val consumerMock = mock<Consumer<PumpEvent, TestTx>>()

        val updateProcess = UpdateContractSummaryProcess(storageMock, deltaProcessorMock, deltaMergerMock, SimpleMeterRegistry(), "")

        updateProcess.onMessage(recordsToProcess, consumerMock)

        inOrder(deltaProcessorMock, deltaMergerMock, storageMock, consumerMock) {
            verify(deltaProcessorMock, times(1)).affectedContracts(recordsToProcess)
            verify(storageMock, times(1)).findAllByIdIn(setOf(testTx.contract))
            verify(deltaProcessorMock, times(1)).recordToDeltas(testRecord)
            verify(deltaMergerMock, times(1)).mergeDeltas(listOf(testDelta), emptyMap())
            verify(storageMock, times(1)).insertIfNotRecord(testMergedDelta.createSummary())
            verify(consumerMock, times(1)).commitSync()
            verify(storageMock, times(1)).findById(testTx.contract)
            verify(storageMock, times(1)).commitUpdate(testTx.contract, 1)
            verifyNoMoreInteractions()
        }

    }


    @Test
    @Suppress("LongMethod")
    fun normalFlowWithContractsInDbTest() {

        val testTx1 = TestTx("a", BigDecimal.ONE)
        val testTx2 = TestTx("b", BigDecimal.ONE)
        val testTx3 = TestTx("c", BigDecimal.ONE)

        val testRecord1 = ConsumerRecord<PumpEvent, TestTx>("TEST_TX", 0, 2, PumpEvent.NEW_BLOCK, testTx1)
        val testRecord2 = ConsumerRecord<PumpEvent, TestTx>("TEST_TX", 0, 2, PumpEvent.NEW_BLOCK, testTx2)
        val testRecord3 = ConsumerRecord<PumpEvent, TestTx>("TEST_TX", 0, 2, PumpEvent.NEW_BLOCK, testTx3)

        val recordsToProcess = listOf(testRecord1, testRecord2, testRecord3)

        val testDelta1 = TestDelta("a", "TEST_TX", 0, 0)
        val testDelta2 = TestDelta("b", "TEST_TX", 0, 0)
        val testDelta3 = TestDelta("c", "TEST_TX", 0, 0)

        val testMergedDelta1 = TestDelta("a", "TEST_TX", 0, 0)
        val testMergedDelta2 = TestDelta("b", "TEST_TX", 0, 0)
        val testMergedDelta3 = TestDelta("c", "TEST_TX", 0, 0)

        val contracts = setOf(testTx1.contract, testTx2.contract, testTx3.contract)

        val contractSummary1 = TestCqlContractSummary("a", 0, 0, 0, "TEST_TX", true)
        val contractSummary2 = TestCqlContractSummary("b", 0, 0, 0, "TEST_TX", false)
        val contractSummary3 = TestCqlContractSummary("c", 0, 0, 0, "TEST_BLOCK", false)

        val existingContractSummaries = listOf(contractSummary1, contractSummary2, contractSummary3)
        val existingContractSummariesMap = listOf(contractSummary1, contractSummary2, contractSummary3)
            .groupBy { a -> a.hash }.map { (k, v) -> k to v.first() }.toMap()

        val contractSummaryNew1 = TestCqlContractSummary("a", 1, 0, 0, "TEST_BLOCK", true)
        val contractSummaryNew3 = TestCqlContractSummary("c", 0, 0, 0, "TEST_BLOCK", true)


        val contractSummaryFinal1 = TestCqlContractSummary("a", 1, 0, 0, "TEST_TX", false)
        val contractSummaryFinal2 = TestCqlContractSummary("b", 0, 0, 0, "TEST_TX", false)
        val contractSummaryFinal3 = TestCqlContractSummary("c", 0, 0, 0, "TEST_TX", false)


        val deltaProcessorMock = mock<DeltaProcessor<TestTx, TestCqlContractSummary, TestDelta>> {
            on { affectedContracts(recordsToProcess) }.thenReturn(contracts)
            on { recordToDeltas(testRecord1) }.thenReturn(listOf(testDelta1))
            on { recordToDeltas(testRecord2) }.thenReturn(listOf(testDelta2))
            on { recordToDeltas(testRecord3) }.thenReturn(listOf(testDelta3))
        }

        val storageMock = mock<ContractSummaryStorage<TestCqlContractSummary>> {
            on { findAllByIdIn(contracts) }.thenReturn(Flux.fromIterable(existingContractSummaries))

            on { update(testDelta1.updateSummary(contractSummary1), 0) }.thenReturn(Mono.just(false))
            on { update(testDelta1.updateSummary(contractSummaryNew1), 1) }.thenReturn(Mono.just(true))
            on { findById(testTx1.contract) }.thenReturn(Mono.just(contractSummaryNew1)).thenReturn(Mono.just(contractSummaryFinal1))

            on { update(testDelta2.updateSummary(contractSummary2), 0) }.thenReturn(Mono.just(true))
            on { findById(testTx2.contract) }.thenReturn(Mono.just(contractSummaryFinal2))

            on { update(testDelta3.updateSummary(contractSummaryNew3), 0) }.thenReturn(Mono.just(true))
            on { findById(testTx3.contract) }.thenReturn(Mono.just(contractSummaryNew3)).thenReturn(Mono.just(contractSummaryFinal3))

            on { commitUpdate(testTx1.contract, 2) }.thenReturn(Mono.just(true))
            on { commitUpdate(testTx2.contract, 1) }.thenReturn(Mono.just(true))
            on { commitUpdate(testTx3.contract, 1) }.thenReturn(Mono.just(true))
        }

        val deltaMergerMock = mock<DeltaMerger<TestDelta>> {
            on { mergeDeltas(listOf(testDelta1), existingContractSummariesMap) }.thenReturn(testMergedDelta1)
            on { mergeDeltas(listOf(testDelta2), existingContractSummariesMap) }.thenReturn(testMergedDelta2)
            on { mergeDeltas(listOf(testDelta3), existingContractSummariesMap) }.thenReturn(testMergedDelta3)
        }

        val consumerMock = mock<Consumer<PumpEvent, TestTx>>()

        val updateProcess = UpdateContractSummaryProcess(storageMock, deltaProcessorMock, deltaMergerMock, SimpleMeterRegistry(), "")

        updateProcess.onMessage(recordsToProcess, consumerMock)

        inOrder(deltaProcessorMock, deltaMergerMock, storageMock, consumerMock) {

            verify(deltaProcessorMock, times(1)).affectedContracts(recordsToProcess)

            verify(storageMock, times(1)).findAllByIdIn(contracts)

            verify(deltaProcessorMock, times(1)).recordToDeltas(testRecord1)
            verify(deltaProcessorMock, times(1)).recordToDeltas(testRecord2)
            verify(deltaProcessorMock, times(1)).recordToDeltas(testRecord3)

            verify(deltaMergerMock, times(1)).mergeDeltas(listOf(testDelta1), existingContractSummariesMap)
            verify(deltaMergerMock, times(1)).mergeDeltas(listOf(testDelta2), existingContractSummariesMap)
            verify(deltaMergerMock, times(1)).mergeDeltas(listOf(testDelta3), existingContractSummariesMap)

            verify(consumerMock, times(1)).commitSync()

            verify(storageMock, times(1)).findById(testTx1.contract)
            verify(storageMock, times(1)).findById(testTx2.contract)
            verify(storageMock, times(1)).findById(testTx3.contract)

            verify(storageMock, times(1)).commitUpdate(testTx1.contract, 2)
            verify(storageMock, times(1)).commitUpdate(testTx2.contract, 1)
            verify(storageMock, times(1)).commitUpdate(testTx3.contract, 1)

            verifyNoMoreInteractions()
        }

        verify(storageMock, times(1)).update(testDelta1.updateSummary(contractSummary1), 0)
        verify(storageMock, times(1)).update(testDelta1.updateSummary(contractSummaryNew1), 1)
        verify(storageMock, times(1)).update(testDelta2.updateSummary(contractSummary2), 0)
        verify(storageMock, times(1)).update(testDelta3.updateSummary(contractSummaryNew3), 0)

        verify(storageMock, times(2)).findById(testTx1.contract)
        verify(storageMock, times(1)).findById(testTx2.contract)
        verify(storageMock, times(2)).findById(testTx3.contract)
    }

}