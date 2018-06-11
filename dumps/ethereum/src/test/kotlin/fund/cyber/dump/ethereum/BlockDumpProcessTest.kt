package fund.cyber.dump.ethereum

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.repository.EthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

class BlockDumpProcessTest {


    private val chainInfo = ChainInfo(ChainFamily.ETHEREUM)

    //        --- D --- E --- G --- I
    //A --- B --- C --- F --- H
    @Test
    @Suppress("LongMethod")
    fun testWithDroppedBlocks() {

        val blockC = block("C","B", 2, "miner3")
        val blockD = block("D", "B", 2, "miner4")
        val blockE = block("E", "D", 3, "miner5")
        val blockF = block("F", "C", 3, "miner6")
        val blockG = block("G", "E", 4, "miner7")
        val blockH = block("H", "F", 4, "miner8")
        val blockI = block("I", "G", 5, "miner9")


        val record1 = record(PumpEvent.NEW_BLOCK, blockH)
        val record2 = record(PumpEvent.DROPPED_BLOCK, blockH)
        val record3 = record(PumpEvent.DROPPED_BLOCK, blockF)
        val record4 = record(PumpEvent.DROPPED_BLOCK, blockC)
        val record5 = record(PumpEvent.NEW_BLOCK, blockD)
        val record6 = record(PumpEvent.NEW_BLOCK, blockE)
        val record7 = record(PumpEvent.NEW_BLOCK, blockG)
        val record8 = record(PumpEvent.NEW_BLOCK, blockI)


        val blockRepository = mock<EthereumBlockRepository> {
            on { save(any<CqlEthereumBlock>()) }.thenReturn(Mono.empty<CqlEthereumBlock>())
            on { delete(any()) }.thenReturn(Mono.empty<Void>())
        }
        val contractMinedBlockRepository = mock<EthereumContractMinedBlockRepository> {
            on { save(any<CqlEthereumContractMinedBlock>()) }.thenReturn(Mono.empty<CqlEthereumContractMinedBlock>())
            on { delete(any()) }.thenReturn(Mono.empty<Void>())
        }

        val blockDumpProcess = BlockDumpProcess(blockRepository, contractMinedBlockRepository, chainInfo)

        blockDumpProcess.onMessage(listOf(record1, record2, record3, record4, record5, record6, record7, record8))


        verify(blockRepository, times(1)).save(CqlEthereumBlock(blockH))
        verify(blockRepository, times(1)).save(CqlEthereumBlock(blockD))
        verify(blockRepository, times(1)).save(CqlEthereumBlock(blockE))
        verify(blockRepository, times(1)).save(CqlEthereumBlock(blockG))
        verify(blockRepository, times(1)).save(CqlEthereumBlock(blockI))

        verify(blockRepository, times(1)).delete(CqlEthereumBlock(blockH))
        verify(blockRepository, times(1)).delete(CqlEthereumBlock(blockF))
        verify(blockRepository, times(1)).delete(CqlEthereumBlock(blockC))

        verify(contractMinedBlockRepository, times(1)).save(CqlEthereumContractMinedBlock(blockH))
        verify(contractMinedBlockRepository, times(1)).save(CqlEthereumContractMinedBlock(blockD))
        verify(contractMinedBlockRepository, times(1)).save(CqlEthereumContractMinedBlock(blockE))
        verify(contractMinedBlockRepository, times(1)).save(CqlEthereumContractMinedBlock(blockG))
        verify(contractMinedBlockRepository, times(1)).save(CqlEthereumContractMinedBlock(blockI))


        verify(contractMinedBlockRepository, times(1)).delete(CqlEthereumContractMinedBlock(blockH))
        verify(contractMinedBlockRepository, times(1)).delete(CqlEthereumContractMinedBlock(blockF))
        verify(contractMinedBlockRepository, times(1)).delete(CqlEthereumContractMinedBlock(blockC))


    }
    
    fun block(hash: String, parentHash: String, number: Long, miner: String) = EthereumBlock(
        hash = hash, number = number, parentHash = parentHash, txNumber = 158, minerContractHash = miner,
        difficulty = BigInteger("0"), totalDifficulty = BigInteger("0"), size = 0,
        unclesReward = BigDecimal("0"), blockReward = BigDecimal("0"),
        txFees = BigDecimal("0"), gasUsed = 0, gasLimit = 0,
        timestamp = Instant.now(), logsBloom = "", transactionsRoot = "", stateRoot = "",
        sha3Uncles = "", nonce = 1, receiptsRoot = "", extraData = "", uncles = emptyList()
    )

    fun record(event: PumpEvent, block: EthereumBlock) =
        ConsumerRecord<PumpEvent, EthereumBlock>(EthereumFamilyChain.ETHEREUM.blockPumpTopic, 0, 0, event, block)


}
