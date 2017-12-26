package fund.cyber.pump

import fund.cyber.node.common.Chain
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

@DisplayName("Common blockchain mechanism: ")
class EventDrivenChainPumpTest {
    private val input = listOf(
            SimpleBundle(hash = "0", parentHash = "", number = 0),
            SimpleBundle(hash = "1", parentHash = "0", number = 1),
            SimpleBundle(hash = "2", parentHash = "1", number = 2),
            SimpleBundle(hash = "3", parentHash = "2", number = 3),
            SimpleBundle(hash = "4", parentHash = "3", number = 4),
            SimpleBundle(hash = "5", parentHash = "4", number = 5),
            SimpleBundle(hash = "6", parentHash = "5", number = 6),
            SimpleBundle(hash = "7", parentHash = "6", number = 7),
            SimpleBundle(hash = "8", parentHash = "7", number = 8),
            SimpleBundle(hash = "9", parentHash = "8", number = 9),
            SimpleBundle(hash = "10", parentHash = "9", number = 10),
            SimpleBundle(hash = "11", parentHash = "10", number = 11),
            SimpleBundle(hash = "12", parentHash = "11", number = 12),
            SimpleBundle(hash = "13", parentHash = "12", number = 13),
            SimpleBundle(hash = "14", parentHash = "13", number = 14),
            SimpleBundle(hash = "15", parentHash = "14", number = 15)
    )

    @Test
    @DisplayName("Right reorganization events sequence")
    fun reorganizationTest() {

        val altInput = listOf(
                SimpleBundle(hash = "alt_0", parentHash = "", number = 0),
                SimpleBundle(hash = "alt_1", parentHash = "alt_0", number = 1),
                SimpleBundle(hash = "alt_2", parentHash = "1", number = 2),
                SimpleBundle(hash = "alt_3", parentHash = "alt_2", number = 3),
                SimpleBundle(hash = "alt_4", parentHash = "alt_3", number = 4)
        )

        val context = ConvertContext(
                blockByNumber = {number ->
                    input[number.toInt()]
                }
        )

        var events = listOf<String>()

        for (i in 0..7) {
            val bundle = if (i in 2..3) altInput[i] else input[i]
            convert(bundle, context)
                    .forEach { event ->
                        if (event is CommitBlock) events += "Commit " + event.bundle.hash
                        if (event is RevertBlock) events += "Revert " + event.bundle.hash
                    }
        }

        Assertions.assertArrayEquals(
                events.toTypedArray(),
                arrayOf("Commit 0", "Commit 1", "Commit alt_2", "Commit alt_3", "Revert alt_3", "Revert alt_2",
                        "Commit 2", "Commit 3", "Commit 4", "Commit 5", "Commit 6", "Commit 7")
        )
    }

    private class SimpleBundle(
            override val chain: Chain = Chain.BITCOIN,
            override val hash: String,
            override val parentHash: String,
            override val number: Long
    ) : BlockBundle
}