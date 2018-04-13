package fund.cyber.pump.common.node

import com.nhaarman.mockito_kotlin.mock
import fund.cyber.common.StackCache
import fund.cyber.search.model.events.PumpEvent
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

data class TestBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        override val blockSize: Int
) : BlockBundle

class BlockBundleEventGeneratorTest {

    @Test
    fun normalFlowTest() {
        val blockA = TestBlockBundle("a", "", 1, 1)
        val blockB = TestBlockBundle("b", "a", 2, 1)
        val blockC = TestBlockBundle("c", "b", 3, 1)
        val blockD = TestBlockBundle("d", "c", 4, 1)

        val history = StackCache<TestBlockBundle>(20)

        history.push(blockA)
        history.push(blockB)
        history.push(blockC)

        val blockBundleMapper = ChainReorganizationBlockBundleEventGenerator<TestBlockBundle>(mock(), SimpleMeterRegistry())


        val result = blockBundleMapper.generate(blockD, history)

        Assertions.assertThat(result.size).isEqualTo(1)
        Assertions.assertThat(result).containsExactly(
                PumpEvent.NEW_BLOCK to blockD
        )

    }

    @Test
    @Suppress("LongMethod")
    fun chainReorganizationTest() {
        val blockA = TestBlockBundle("a", "", 1, 1)
        val blockB = TestBlockBundle("b", "a", 2, 1)
        val blockC = TestBlockBundle("c", "b", 3, 1)
        val blockD = TestBlockBundle("d", "c", 4, 1)
        val blockE = TestBlockBundle("e", "d", 5, 1)
        val blockF = TestBlockBundle("f", "b", 3, 1)
        val blockG = TestBlockBundle("g", "f", 4, 1)
        val blockH = TestBlockBundle("h", "g", 5, 1)
        val blockK = TestBlockBundle("k", "h", 6, 1)

        val history = StackCache<TestBlockBundle>(20)

        history.push(blockA)
        history.push(blockB)
        history.push(blockC)
        history.push(blockD)
        history.push(blockE)

        val blockchainInterface = mock<FlowableBlockchainInterface<TestBlockBundle>> {
            on { blockBundleByNumber(3) }.thenReturn(blockF)
            on { blockBundleByNumber(4) }.thenReturn(blockG)
            on { blockBundleByNumber(5) }.thenReturn(blockH)
        }

        val blockBundleMapper = ChainReorganizationBlockBundleEventGenerator(blockchainInterface, SimpleMeterRegistry())


        val result = blockBundleMapper.generate(blockK, history)

        Assertions.assertThat(result.size).isEqualTo(7)
        Assertions.assertThat(result.filter { pair -> pair.first == PumpEvent.DROPPED_BLOCK }.size).isEqualTo(3)
        Assertions.assertThat(result.filter { pair -> pair.first == PumpEvent.NEW_BLOCK }.size).isEqualTo(4)
        Assertions.assertThat(result).containsExactly(
                PumpEvent.DROPPED_BLOCK to blockE,
                PumpEvent.DROPPED_BLOCK to blockD,
                PumpEvent.DROPPED_BLOCK to blockC,
                PumpEvent.NEW_BLOCK to blockF,
                PumpEvent.NEW_BLOCK to blockG,
                PumpEvent.NEW_BLOCK to blockH,
                PumpEvent.NEW_BLOCK to blockK
        )


    }

    @Test
    @Suppress("LongMethod")
    fun chainReorganizationEmptyStackTest() {
        val blockA = TestBlockBundle("a", "", 1, 1)
        val blockB = TestBlockBundle("b", "a", 2, 1)
        val blockC = TestBlockBundle("c", "b", 3, 1)
        val blockD = TestBlockBundle("d", "c", 4, 1)
        val blockE = TestBlockBundle("e", "d", 5, 1)
        val blockF = TestBlockBundle("f", "b", 3, 1)
        val blockG = TestBlockBundle("g", "f", 4, 1)
        val blockH = TestBlockBundle("h", "g", 5, 1)
        val blockK = TestBlockBundle("k", "h", 6, 1)

        val history = StackCache<TestBlockBundle>(2)

        history.push(blockA)
        history.push(blockB)
        history.push(blockC)
        history.push(blockD)
        history.push(blockE)

        val blockchainInterface = mock<FlowableBlockchainInterface<TestBlockBundle>> {
            on { blockBundleByNumber(3) }.thenReturn(blockF)
            on { blockBundleByNumber(4) }.thenReturn(blockG)
            on { blockBundleByNumber(5) }.thenReturn(blockH)
        }

        val blockBundleMapper = ChainReorganizationBlockBundleEventGenerator(blockchainInterface, SimpleMeterRegistry())

        Assertions
                .assertThatExceptionOfType(HistoryStackIsEmptyException::class.java)
                .isThrownBy { blockBundleMapper.generate(blockK, history) }

    }

}
