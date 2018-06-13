package fund.cyber.dump.bitcoin

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractMinedBlock
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractMinedBlockRepository
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

class BlockDumpProcessTest {


    private val chainInfo = ChainInfo(ChainFamily.BITCOIN)

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


        val blockRepository = mock<BitcoinBlockRepository> {
            on { save(any<CqlBitcoinBlock>()) }.thenReturn(Mono.empty<CqlBitcoinBlock>())
            on { delete(any()) }.thenReturn(Mono.empty<Void>())
        }
        val contractMinedBlockRepository = mock<BitcoinContractMinedBlockRepository> {
            on { save(any<CqlBitcoinContractMinedBlock>()) }.thenReturn(Mono.empty<CqlBitcoinContractMinedBlock>())
            on { delete(any()) }.thenReturn(Mono.empty<Void>())
        }

        val blockDumpProcess = BlockDumpProcess(blockRepository, contractMinedBlockRepository, chainInfo)

        blockDumpProcess.onMessage(listOf(record1, record2, record3, record4, record5, record6, record7, record8))


        verify(blockRepository, times(1)).save(CqlBitcoinBlock(blockH))
        verify(blockRepository, times(1)).save(CqlBitcoinBlock(blockD))
        verify(blockRepository, times(1)).save(CqlBitcoinBlock(blockE))
        verify(blockRepository, times(1)).save(CqlBitcoinBlock(blockG))
        verify(blockRepository, times(1)).save(CqlBitcoinBlock(blockI))

        verify(blockRepository, times(1)).delete(CqlBitcoinBlock(blockH))
        verify(blockRepository, times(1)).delete(CqlBitcoinBlock(blockF))
        verify(blockRepository, times(1)).delete(CqlBitcoinBlock(blockC))

        verify(contractMinedBlockRepository, times(1)).save(CqlBitcoinContractMinedBlock(blockH))
        verify(contractMinedBlockRepository, times(1)).save(CqlBitcoinContractMinedBlock(blockD))
        verify(contractMinedBlockRepository, times(1)).save(CqlBitcoinContractMinedBlock(blockE))
        verify(contractMinedBlockRepository, times(1)).save(CqlBitcoinContractMinedBlock(blockG))
        verify(contractMinedBlockRepository, times(1)).save(CqlBitcoinContractMinedBlock(blockI))


        verify(contractMinedBlockRepository, times(1)).delete(CqlBitcoinContractMinedBlock(blockH))
        verify(contractMinedBlockRepository, times(1)).delete(CqlBitcoinContractMinedBlock(blockF))
        verify(contractMinedBlockRepository, times(1)).delete(CqlBitcoinContractMinedBlock(blockC))

    }
    
    fun block(hash: String, parentHash: String, number: Long, miner: String) = BitcoinBlock(
        hash = hash, height = number, txNumber = 158, minerContractHash = miner, difficulty = BigInteger("0"),
        size = 0, blockReward = BigDecimal("0"), txFees = BigDecimal("0"), time = Instant.ofEpochMilli(1000000),
        coinbaseData = "0x", nonce = 0, merkleroot = "0x", version = 0, weight = 0,
        totalOutputsAmount = BigDecimal.ZERO, bits = "0x", parentHash = parentHash
    )

    fun record(event: PumpEvent, block: BitcoinBlock) =
        ConsumerRecord<PumpEvent, BitcoinBlock>(chainInfo.blockPumpTopic, 0, 0, event, block)


}
