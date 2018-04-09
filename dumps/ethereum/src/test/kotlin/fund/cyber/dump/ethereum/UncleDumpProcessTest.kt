package fund.cyber.dump.ethereum

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressMinedUncle
import fund.cyber.cassandra.ethereum.model.CqlEthereumUncle
import fund.cyber.cassandra.ethereum.repository.EthereumAddressUncleRepository
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.unclePumpTopic
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Instant

class UncleDumpProcessTest {


    //        --- D --- E --- G --- I
    //A --- B --- C --- F --- H
    @Test
    @Suppress("LongMethod")
    fun testWithDroppedUncles() {

        val uncleC = EthereumUncle(
                hash = "C", position = 0, number = 0, timestamp = Instant.now(), blockNumber = 0,
                blockTime = Instant.now(), blockHash = "C", miner = "miner", uncleReward = BigDecimal("0")
        )

        val uncleD = EthereumUncle(
                hash = "D", position = 0, number = 0, timestamp = Instant.now(), blockNumber = 0,
                blockTime = Instant.now(), blockHash = "D", miner = "miner", uncleReward = BigDecimal("0")
        )

        val uncleE = EthereumUncle(
                hash = "E", position = 0, number = 0, timestamp = Instant.now(), blockNumber = 0,
                blockTime = Instant.now(), blockHash = "E", miner = "miner", uncleReward = BigDecimal("0")
        )

        val uncleF = EthereumUncle(
                hash = "F", position = 0, number = 0, timestamp = Instant.now(), blockNumber = 0,
                blockTime = Instant.now(), blockHash = "F", miner = "miner", uncleReward = BigDecimal("0")
        )
        val uncleG = EthereumUncle(
                hash = "G", position = 0, number = 0, timestamp = Instant.now(), blockNumber = 0,
                blockTime = Instant.now(), blockHash = "G", miner = "miner", uncleReward = BigDecimal("0")
        )

        val uncleH = EthereumUncle(
                hash = "H", position = 0, number = 0, timestamp = Instant.now(), blockNumber = 0,
                blockTime = Instant.now(), blockHash = "H", miner = "miner", uncleReward = BigDecimal("0")
        )

        val uncleI = EthereumUncle(
                hash = "I", position = 0, number = 0, timestamp = Instant.now(), blockNumber = 0,
                blockTime = Instant.now(), blockHash = "I", miner = "miner", uncleReward = BigDecimal("0")
        )


        val record1 = ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, uncleH)
        val record2 = ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0,
                0, PumpEvent.DROPPED_BLOCK, uncleH)
        val record3 = ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0,
                0, PumpEvent.DROPPED_BLOCK, uncleF)
        val record4 = ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0,
                0, PumpEvent.DROPPED_BLOCK, uncleC)
        val record5 = ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, uncleD)
        val record6 = ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, uncleE)
        val record7 = ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, uncleG)
        val record8 = ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, uncleI)

        val uncleRepository = mock<EthereumUncleRepository> {
            on { saveAll(any<Iterable<CqlEthereumUncle>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlEthereumUncle>>()) }.thenReturn(Mono.empty())
        }
        val addressUncleRepository = mock<EthereumAddressUncleRepository> {
            on { saveAll(any<Iterable<CqlEthereumAddressMinedUncle>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlEthereumAddressMinedUncle>>()) }.thenReturn(Mono.empty())
        }

        val blockDumpProcess = UncleDumpProcess(uncleRepository, addressUncleRepository, EthereumFamilyChain.ETHEREUM,
                SimpleMeterRegistry())

        blockDumpProcess.onMessage(listOf(record1, record2, record3, record4, record5, record6, record7, record8))


        verify(uncleRepository, times(1))
                .saveAll(listOf(CqlEthereumUncle(uncleD), CqlEthereumUncle(uncleE),
                        CqlEthereumUncle(uncleG), CqlEthereumUncle(uncleI)))
        verify(uncleRepository, times(1))
                .deleteAll(listOf(CqlEthereumUncle(uncleF), CqlEthereumUncle(uncleC)))

        verify(addressUncleRepository, times(1))
                .saveAll(listOf(CqlEthereumAddressMinedUncle(uncleD), CqlEthereumAddressMinedUncle(uncleE),
                        CqlEthereumAddressMinedUncle(uncleG), CqlEthereumAddressMinedUncle(uncleI)))
        verify(addressUncleRepository, times(1))
                .deleteAll(listOf(CqlEthereumAddressMinedUncle(uncleF), CqlEthereumAddressMinedUncle(uncleC)))


    }


}
