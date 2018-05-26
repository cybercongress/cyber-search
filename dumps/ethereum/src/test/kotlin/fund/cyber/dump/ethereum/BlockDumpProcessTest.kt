package fund.cyber.dump.ethereum

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.repository.EthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
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


    //        --- D --- E --- G --- I
    //A --- B --- C --- F --- H
    @Test
    @Suppress("LongMethod")
    fun testWithDroppedBlocks() {

        val blockC = EthereumBlock(
                hash = "C", number = 2,
                parentHash = "B",
                txNumber = 158, minerContractHash = "miner3",
                difficulty = BigInteger("0"),
                totalDifficulty = BigInteger("0"), size = 0,
                unclesReward = BigDecimal("0"), blockReward = BigDecimal("0"),
                txFees = BigDecimal("0"), gasUsed = 0, gasLimit = 0,
                timestamp = Instant.now(), logsBloom = "", transactionsRoot = "", stateRoot = "",
                sha3Uncles = "",
                nonce = 1, receiptsRoot = "", extraData = "", uncles = emptyList()
        )

        val blockD = EthereumBlock(
                hash = "D", number = 2,
                parentHash = "B",
                txNumber = 158, minerContractHash = "miner4",
                difficulty = BigInteger("0"),
                totalDifficulty = BigInteger("0"), size = 0,
                unclesReward = BigDecimal("0"), blockReward = BigDecimal("0"),
                txFees = BigDecimal("0"), gasUsed = 0, gasLimit = 0,
                timestamp = Instant.now(), logsBloom = "", transactionsRoot = "", stateRoot = "",
                sha3Uncles = "",
                nonce = 1, receiptsRoot = "", extraData = "", uncles = emptyList()
        )

        val blockE = EthereumBlock(
                hash = "E", number = 3,
                parentHash = "D",
                txNumber = 158, minerContractHash = "miner5",
                difficulty = BigInteger("0"),
                totalDifficulty = BigInteger("0"), size = 0,
                unclesReward = BigDecimal("0"), blockReward = BigDecimal("0"),
                txFees = BigDecimal("0"), gasUsed = 0, gasLimit = 0,
                timestamp = Instant.now(), logsBloom = "", transactionsRoot = "", stateRoot = "",
                sha3Uncles = "",
                nonce = 1, receiptsRoot = "", extraData = "", uncles = emptyList()
        )

        val blockF = EthereumBlock(
                hash = "F", number = 3,
                parentHash = "C",
                txNumber = 158, minerContractHash = "miner6",
                difficulty = BigInteger("0"),
                totalDifficulty = BigInteger("0"), size = 0,
                unclesReward = BigDecimal("0"), blockReward = BigDecimal("0"),
                txFees = BigDecimal("0"), gasUsed = 0, gasLimit = 0,
                timestamp = Instant.now(), logsBloom = "", transactionsRoot = "", stateRoot = "",
                sha3Uncles = "",
                nonce = 1, receiptsRoot = "", extraData = "", uncles = emptyList()
        )

        val blockG = EthereumBlock(
                hash = "G", number = 4,
                parentHash = "E",
                txNumber = 158, minerContractHash = "miner7",
                difficulty = BigInteger("0"),
                totalDifficulty = BigInteger("0"), size = 0,
                unclesReward = BigDecimal("0"), blockReward = BigDecimal("0"),
                txFees = BigDecimal("0"), gasUsed = 0, gasLimit = 0,
                timestamp = Instant.now(), logsBloom = "", transactionsRoot = "", stateRoot = "",
                sha3Uncles = "",
                nonce = 1, receiptsRoot = "", extraData = "", uncles = emptyList()
        )

        val blockH = EthereumBlock(
                hash = "H", number = 4,
                parentHash = "F",
                txNumber = 158, minerContractHash = "miner8",
                difficulty = BigInteger("0"),
                totalDifficulty = BigInteger("0"), size = 0,
                unclesReward = BigDecimal("0"), blockReward = BigDecimal("0"),
                txFees = BigDecimal("0"), gasUsed = 0, gasLimit = 0,
                timestamp = Instant.now(), logsBloom = "", transactionsRoot = "", stateRoot = "",
                sha3Uncles = "",
                nonce = 1, receiptsRoot = "", extraData = "", uncles = emptyList()
        )

        val blockI = EthereumBlock(
                hash = "I", number = 5,
                parentHash = "G",
                txNumber = 158, minerContractHash = "miner9",
                difficulty = BigInteger("0"),
                totalDifficulty = BigInteger("0"), size = 0,
                unclesReward = BigDecimal("0"), blockReward = BigDecimal("0"),
                txFees = BigDecimal("0"), gasUsed = 0, gasLimit = 0,
                timestamp = Instant.now(), logsBloom = "", transactionsRoot = "", stateRoot = "",
                sha3Uncles = "",
                nonce = 1, receiptsRoot = "", extraData = "", uncles = emptyList()
        )


        val record1 = ConsumerRecord<PumpEvent, EthereumBlock>(EthereumFamilyChain.ETHEREUM.blockPumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, blockH)
        val record2 = ConsumerRecord<PumpEvent, EthereumBlock>(EthereumFamilyChain.ETHEREUM.blockPumpTopic, 0,
                0, PumpEvent.DROPPED_BLOCK, blockH)
        val record3 = ConsumerRecord<PumpEvent, EthereumBlock>(EthereumFamilyChain.ETHEREUM.blockPumpTopic, 0,
                0, PumpEvent.DROPPED_BLOCK, blockF)
        val record4 = ConsumerRecord<PumpEvent, EthereumBlock>(EthereumFamilyChain.ETHEREUM.blockPumpTopic, 0,
                0, PumpEvent.DROPPED_BLOCK, blockC)
        val record5 = ConsumerRecord<PumpEvent, EthereumBlock>(EthereumFamilyChain.ETHEREUM.blockPumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, blockD)
        val record6 = ConsumerRecord<PumpEvent, EthereumBlock>(EthereumFamilyChain.ETHEREUM.blockPumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, blockE)
        val record7 = ConsumerRecord<PumpEvent, EthereumBlock>(EthereumFamilyChain.ETHEREUM.blockPumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, blockG)
        val record8 = ConsumerRecord<PumpEvent, EthereumBlock>(EthereumFamilyChain.ETHEREUM.blockPumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, blockI)


        val blockRepository = mock<EthereumBlockRepository> {
            on { save(any<CqlEthereumBlock>()) }.doReturn(Mono.empty<CqlEthereumBlock>())
            on { delete(any()) }.thenReturn(Mono.empty<Void>())
        }
        val contractMinedBlockRepository = mock<EthereumContractMinedBlockRepository> {
            on { save(any<CqlEthereumContractMinedBlock>()) }.thenReturn(Mono.empty<CqlEthereumContractMinedBlock>())
            on { delete(any()) }.thenReturn(Mono.empty<Void>())
        }

        val blockDumpProcess = BlockDumpProcess(blockRepository, contractMinedBlockRepository,
            EthereumFamilyChain.ETHEREUM)

        blockDumpProcess.onMessage(listOf(record1, record2, record3, record4, record5, record6, record7, record8))


        verify(blockRepository, times(1)).save(CqlEthereumBlock(blockD))
        verify(blockRepository, times(1)).save(CqlEthereumBlock(blockE))
        verify(blockRepository, times(1)).save(CqlEthereumBlock(blockG))
        verify(blockRepository, times(1)).save(CqlEthereumBlock(blockI))

        verify(blockRepository, times(1)).delete(CqlEthereumBlock(blockF))
        verify(blockRepository, times(1)).delete(CqlEthereumBlock(blockC))

        verify(contractMinedBlockRepository, times(1)).save(CqlEthereumContractMinedBlock(blockD))
        verify(contractMinedBlockRepository, times(1)).save(CqlEthereumContractMinedBlock(blockE))
        verify(contractMinedBlockRepository, times(1)).save(CqlEthereumContractMinedBlock(blockG))
        verify(contractMinedBlockRepository, times(1)).save(CqlEthereumContractMinedBlock(blockI))


        verify(contractMinedBlockRepository, times(1)).delete(CqlEthereumContractMinedBlock(blockF))
        verify(contractMinedBlockRepository, times(1)).delete(CqlEthereumContractMinedBlock(blockC))


    }


}
