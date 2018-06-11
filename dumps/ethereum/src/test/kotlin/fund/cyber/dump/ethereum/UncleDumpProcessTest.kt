package fund.cyber.dump.ethereum

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedUncle
import fund.cyber.cassandra.ethereum.model.CqlEthereumUncle
import fund.cyber.cassandra.ethereum.repository.EthereumContractUncleRepository
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.unclePumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Instant

class UncleDumpProcessTest {


    private val chainInfo = ChainInfo(ChainFamily.ETHEREUM)


    //        --- D --- E --- G --- I
    //A --- B --- C --- F --- H
    @Test
    @Suppress("LongMethod")
    fun testWithDroppedUncles() {

        val uncleC = uncle("C")
        val uncleD = uncle("D")
        val uncleE = uncle("E")
        val uncleF = uncle("F")
        val uncleG = uncle("G")
        val uncleH = uncle("H")
        val uncleI = uncle("I")


        val record1 = record(PumpEvent.NEW_BLOCK, uncleH)
        val record2 = record(PumpEvent.DROPPED_BLOCK, uncleH)
        val record3 = record(PumpEvent.DROPPED_BLOCK, uncleF)
        val record4 = record(PumpEvent.DROPPED_BLOCK, uncleC)
        val record5 = record(PumpEvent.NEW_BLOCK, uncleD)
        val record6 = record(PumpEvent.NEW_BLOCK, uncleE)
        val record7 = record(PumpEvent.NEW_BLOCK, uncleG)
        val record8 = record(PumpEvent.NEW_BLOCK, uncleI)

        val uncleRepository = mock<EthereumUncleRepository> {
            on { save(any<CqlEthereumUncle>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractUncleRepository = mock<EthereumContractUncleRepository> {
            on { save(any<CqlEthereumContractMinedUncle>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }

        val blockDumpProcess = UncleDumpProcess(uncleRepository, contractUncleRepository, chainInfo)

        blockDumpProcess.onMessage(listOf(record1, record2, record3, record4, record5, record6, record7, record8))


        verify(uncleRepository, times(1)).save(CqlEthereumUncle(uncleH))
        verify(uncleRepository, times(1)).save(CqlEthereumUncle(uncleD))
        verify(uncleRepository, times(1)).save(CqlEthereumUncle(uncleE))
        verify(uncleRepository, times(1)).save(CqlEthereumUncle(uncleG))
        verify(uncleRepository, times(1)).save(CqlEthereumUncle(uncleI))
        verify(uncleRepository, times(1)).delete(CqlEthereumUncle(uncleH))
        verify(uncleRepository, times(1)).delete(CqlEthereumUncle(uncleF))
        verify(uncleRepository, times(1)).delete(CqlEthereumUncle(uncleC))

        verify(contractUncleRepository, times(1)).save(CqlEthereumContractMinedUncle(uncleH))
        verify(contractUncleRepository, times(1)).save(CqlEthereumContractMinedUncle(uncleD))
        verify(contractUncleRepository, times(1)).save(CqlEthereumContractMinedUncle(uncleE))
        verify(contractUncleRepository, times(1)).save(CqlEthereumContractMinedUncle(uncleG))
        verify(contractUncleRepository, times(1)).save(CqlEthereumContractMinedUncle(uncleI))
        verify(contractUncleRepository, times(1)).delete(CqlEthereumContractMinedUncle(uncleH))
        verify(contractUncleRepository, times(1)).delete(CqlEthereumContractMinedUncle(uncleF))
        verify(contractUncleRepository, times(1)).delete(CqlEthereumContractMinedUncle(uncleC))


    }

    fun uncle(hash: String) = EthereumUncle(
        hash = hash, position = 0, number = 0, timestamp = Instant.now(), blockNumber = 0,
        blockTime = Instant.now(), blockHash = hash, miner = "minerContractHash", uncleReward = BigDecimal("0")
    )

    fun record(event: PumpEvent, uncle: EthereumUncle) =
        ConsumerRecord<PumpEvent, EthereumUncle>(EthereumFamilyChain.ETHEREUM.unclePumpTopic, 0, 0, event, uncle)


}
