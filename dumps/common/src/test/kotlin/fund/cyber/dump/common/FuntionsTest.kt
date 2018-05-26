package fund.cyber.dump.common

import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.test.StepVerifier

data class TestData(
    private val hash: String,
    private val blockNumber: Long,
    private val data: String
)

class FunctionsTest {

    @Test
    fun compileOperationsTest() {
        val records = listOf(
            testRecord(PumpEvent.NEW_BLOCK),
            testRecord(PumpEvent.DROPPED_BLOCK),
            testRecord(PumpEvent.NEW_POOL_TX),
            testRecord(PumpEvent.NEW_BLOCK),
            testRecord(PumpEvent.NEW_BLOCK)
        )

        val fluxesToExecute = records.compileOperations { event, _ ->
            return@compileOperations when (event) {
                PumpEvent.NEW_BLOCK -> Flux.fromIterable(listOf("a1", "a2", "a3"))
                PumpEvent.NEW_POOL_TX -> Flux.fromIterable(listOf("b1", "b2"))
                PumpEvent.DROPPED_BLOCK -> Flux.fromIterable(listOf("c1", "c2", "c3"))
            }
        }

        Assertions.assertThat(fluxesToExecute).hasSize(3)

        StepVerifier.create(fluxesToExecute[0])
            .expectNext("a1")
            .expectNext("a2")
            .expectNext("a3")
            .verifyComplete()

        StepVerifier.create(fluxesToExecute[1])
            .expectNext("c1")
            .expectNext("c2")
            .expectNext("c3")
            .expectNext("b1")
            .expectNext("b2")
            .verifyComplete()

        StepVerifier.create(fluxesToExecute[2])
            .expectNext("a1")
            .expectNext("a2")
            .expectNext("a3")
            .expectNext("a1")
            .expectNext("a2")
            .expectNext("a3")
            .verifyComplete()

    }

    @Test
    fun compileOperationsWithNoRecordsTest() {
        val records = emptyList<ConsumerRecord<PumpEvent, TestData>>()

        val fluxesToExecute = records.compileOperations { event, _ ->
            return@compileOperations when (event) {
                PumpEvent.NEW_BLOCK -> Flux.fromIterable(listOf("a1", "a2", "a3"))
                PumpEvent.NEW_POOL_TX -> Flux.fromIterable(listOf("b1", "b2"))
                PumpEvent.DROPPED_BLOCK -> Flux.fromIterable(listOf("c1", "c2", "c3"))
            }
        }

        Assertions.assertThat(fluxesToExecute).isEmpty()
    }

    @Test
    fun compileOperationsWithEmptyRecordsTest() {
        val records = listOf(
            testRecord(PumpEvent.NEW_BLOCK),
            testRecord(PumpEvent.DROPPED_BLOCK),
            testRecord(PumpEvent.NEW_POOL_TX),
            testRecord(PumpEvent.NEW_BLOCK),
            testRecord(PumpEvent.NEW_BLOCK)
        )

        val fluxesToExecute = records.compileOperations { event, _ ->
            return@compileOperations when (event) {
                PumpEvent.NEW_BLOCK -> Flux.empty()
                PumpEvent.NEW_POOL_TX -> Flux.empty<Any>()
                PumpEvent.DROPPED_BLOCK -> Flux.empty()
            }
        }

        Assertions.assertThat(fluxesToExecute).hasSize(3)

        StepVerifier.create(fluxesToExecute[0])
            .verifyComplete()

        StepVerifier.create(fluxesToExecute[1])
            .verifyComplete()

        StepVerifier.create(fluxesToExecute[2])
            .verifyComplete()
    }


    @Test
    fun compileOperationsWithDifferentPublishersTest() {
        val records = listOf(
            testRecord(PumpEvent.NEW_BLOCK),
            testRecord(PumpEvent.DROPPED_BLOCK),
            testRecord(PumpEvent.NEW_POOL_TX),
            testRecord(PumpEvent.NEW_BLOCK),
            testRecord(PumpEvent.NEW_BLOCK)
        )

        val fluxesToExecute = records.compileOperations { event, _ ->
            return@compileOperations when (event) {
                PumpEvent.NEW_BLOCK -> Flux.fromIterable(listOf("a1", "a2", "a3"))
                PumpEvent.NEW_POOL_TX -> Mono.just("b1")
                PumpEvent.DROPPED_BLOCK -> Flux.fromIterable(listOf("c1", "c2", "c3"))
            }
        }

        Assertions.assertThat(fluxesToExecute).hasSize(3)


        StepVerifier.create(fluxesToExecute[0])
            .expectNext("a1")
            .expectNext("a2")
            .expectNext("a3")
            .verifyComplete()

        StepVerifier.create(fluxesToExecute[1])
            .expectNext("c1")
            .expectNext("c2")
            .expectNext("c3")
            .expectNext("b1")
            .verifyComplete()

        StepVerifier.create(fluxesToExecute[2])
            .expectNext("a1")
            .expectNext("a2")
            .expectNext("a3")
            .expectNext("a1")
            .expectNext("a2")
            .expectNext("a3")
            .verifyComplete()
    }

    @Test
    fun hacksWithPublishersTest() {
        val testMono = Mono.just("a")
            .flatMap { _ -> Mono.just(1) }
            .switchIfEmpty(Mono.just(2))

        StepVerifier.create(testMono)
            .expectNext(1)
            .verifyComplete()

        val testMono2 = Mono.just(3)
            .map { 1 } //stub to convert in right type
            .toFlux()
            .switchIfEmpty(Flux.just(2))

        StepVerifier.create(testMono2)
            .expectNext(1)
            .verifyComplete()

        val testFlux3 = Mono.just(4L)
            .map { it -> it as Any } // stub to convert whole Flux to right type
            .toFlux()
            .switchIfEmpty(
                Flux.concat(Flux.just(1), Flux.just("a"))
            )

        StepVerifier.create(testFlux3)
            .expectNext(4L)
            .verifyComplete()

        val testFlux4 = Mono.empty<Long>()
            .map { it -> it as Any } // stub to convert whole Flux to right type
            .toFlux()
            .switchIfEmpty(
                Flux.concat(Flux.just(1), Flux.just("a"))
            )

        StepVerifier.create(testFlux4)
            .expectNext(1)
            .expectNext("a")
            .verifyComplete()
    }

    private fun testRecord(event: PumpEvent) =
        ConsumerRecord("topic", 0, 0, event, TestData("a", 1, "a"))


}
