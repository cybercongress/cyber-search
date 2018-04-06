package fund.cyber.dump.ethereum

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.repository.EthereumAddressMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
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
                txNumber = 158, miner = "miner3",
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
                txNumber = 158, miner = "miner4",
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
                txNumber = 158, miner = "miner5",
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
                txNumber = 158, miner = "miner6",
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
                txNumber = 158, miner = "miner7",
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
                txNumber = 158, miner = "miner8",
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
                txNumber = 158, miner = "miner9",
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
            on { saveAll(any<Iterable<CqlEthereumBlock>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlEthereumBlock>>()) }.thenReturn(Mono.empty())
        }
        val addressMinedBlockRepository = mock<EthereumAddressMinedBlockRepository> {
            on { saveAll(any<Iterable<CqlEthereumAddressMinedBlock>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlEthereumAddressMinedBlock>>()) }.thenReturn(Mono.empty())
        }

        val blockDumpProcess = BlockDumpProcess(blockRepository, addressMinedBlockRepository,
                EthereumFamilyChain.ETHEREUM, SimpleMeterRegistry())

        blockDumpProcess.onMessage(listOf(record1, record2, record3, record4, record5, record6, record7, record8))


        verify(blockRepository, times(1))
                .saveAll(listOf(CqlEthereumBlock(blockD), CqlEthereumBlock(blockE),
                        CqlEthereumBlock(blockG), CqlEthereumBlock(blockI)))
        verify(blockRepository, times(1))
                .deleteAll(listOf(CqlEthereumBlock(blockF), CqlEthereumBlock(blockC)))

        verify(addressMinedBlockRepository, times(1))
                .saveAll(listOf(CqlEthereumAddressMinedBlock(blockD), CqlEthereumAddressMinedBlock(blockE),
                        CqlEthereumAddressMinedBlock(blockG), CqlEthereumAddressMinedBlock(blockI)))
        verify(addressMinedBlockRepository, times(1))
                .deleteAll(listOf(CqlEthereumAddressMinedBlock(blockF), CqlEthereumAddressMinedBlock(blockC)))


    }


}
