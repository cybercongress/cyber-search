package fund.cyber.dump.bitcoin

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.never
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlockTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.BitcoinTxIn
import fund.cyber.search.model.bitcoin.BitcoinTxOut
import fund.cyber.search.model.bitcoin.SignatureScript
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Instant

@Suppress("LargeClass")
class TxDumpProcessTest {


    @Test
    @Suppress("LongMethod")
    fun testWithAllEventsTxs() {

        val txC = tx("C", "C")
        val txD = tx("D", "D")
        val txE = tx("E", "E")
        val txF = tx("F", "F")
        val txG = tx("G", "G")
        val txH = tx("H", "H")
        val txI = tx("I", "I")
        val txK = tx("K", "K")


        val record1 = record(PumpEvent.NEW_BLOCK, txH)
        val record2 = record(PumpEvent.DROPPED_BLOCK, txH)
        val record3 = record(PumpEvent.DROPPED_BLOCK, txF)
        val record4 = record(PumpEvent.DROPPED_BLOCK, txC)
        val record5 = record(PumpEvent.NEW_BLOCK, txD)
        val record6 = record(PumpEvent.NEW_BLOCK, txE)
        val record7 = record(PumpEvent.NEW_BLOCK, txG)
        val record8 = record(PumpEvent.NEW_BLOCK, txI)
        val record9 = record(PumpEvent.NEW_POOL_TX, txK)

        val txRepository = mock<BitcoinTxRepository> {
            on { save(any<CqlBitcoinTx>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.empty())
        }
        val blockTxRepository = mock<BitcoinBlockTxRepository> {
            on { save(any<CqlBitcoinBlockTxPreview>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractTxRepository = mock<BitcoinContractTxRepository> {
            on { saveAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val txDumpProcess = TxDumpProcess(txRepository, contractTxRepository, blockTxRepository,
            BitcoinFamilyChain.BITCOIN)

        txDumpProcess.onMessage(listOf(record1, record2, record3, record4, record5, record6, record7, record8, record9))


        verify(txRepository, times(2)).findById(txH.hash)
        verify(txRepository, times(1)).findById(txF.hash)
        verify(txRepository, times(1)).findById(txC.hash)
        verify(txRepository, times(1)).findById(txD.hash)
        verify(txRepository, times(1)).findById(txE.hash)
        verify(txRepository, times(1)).findById(txG.hash)
        verify(txRepository, times(1)).findById(txI.hash)
        verify(txRepository, times(1)).findById(txK.hash)

        verify(txRepository, times(1)).save(CqlBitcoinTx(txH))
        verify(txRepository, times(1)).save(CqlBitcoinTx(txH.mempoolState()))
        verify(txRepository, times(1)).save(CqlBitcoinTx(txF.mempoolState()))
        verify(txRepository, times(1)).save(CqlBitcoinTx(txC.mempoolState()))
        verify(txRepository, times(1)).save(CqlBitcoinTx(txD))
        verify(txRepository, times(1)).save(CqlBitcoinTx(txE))
        verify(txRepository, times(1)).save(CqlBitcoinTx(txG))
        verify(txRepository, times(1)).save(CqlBitcoinTx(txI))
        verify(txRepository, times(1)).save(CqlBitcoinTx(txK))

        verify(blockTxRepository, times(1)).save(CqlBitcoinBlockTxPreview(txH))
        verify(blockTxRepository, times(1)).delete(CqlBitcoinBlockTxPreview(txH))
        verify(blockTxRepository, times(1)).delete(CqlBitcoinBlockTxPreview(txF))
        verify(blockTxRepository, times(1)).delete(CqlBitcoinBlockTxPreview(txC))
        verify(blockTxRepository, times(1)).save(CqlBitcoinBlockTxPreview(txD))
        verify(blockTxRepository, times(1)).save(CqlBitcoinBlockTxPreview(txE))
        verify(blockTxRepository, times(1)).save(CqlBitcoinBlockTxPreview(txG))
        verify(blockTxRepository, times(1)).save(CqlBitcoinBlockTxPreview(txI))


        listOf(txH, txF, txC, txH.mempoolState(), txD.mempoolState(), txE.mempoolState(), txG.mempoolState(), txI.mempoolState())
            .forEach { tx ->
                verify(contractTxRepository, times(1))
                    .deleteAll(tx.allContractsUsedInTransaction().map { it -> CqlBitcoinContractTxPreview(it, tx) })
            }

        listOf(txH, txD, txE, txG, txI, txK)
            .forEach { tx ->
                verify(contractTxRepository, times(1))
                    .saveAll(tx.allContractsUsedInTransaction().map { it -> CqlBitcoinContractTxPreview(it, tx) })
            }
    }

    @Test
    fun testNewBlockWithoutTxInDb() {
        val txABlock = tx("A", "A")

        val txToSave = CqlBitcoinTx(txABlock)

        val txRepository = mock<BitcoinTxRepository> {
            on { save(any<CqlBitcoinTx>()) }.thenReturn(Mono.just(txToSave))
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.empty())
        }
        val blockTxRepository = mock<BitcoinBlockTxRepository> {
            on { save(any<CqlBitcoinBlockTxPreview>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractTxRepository = mock<BitcoinContractTxRepository> {
            on { saveAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.NEW_BLOCK, txABlock)

        val txDumpProcess = TxDumpProcess(txRepository, contractTxRepository, blockTxRepository,
            BitcoinFamilyChain.BITCOIN)

        txDumpProcess.onMessage(listOf(record))

        verify(txRepository, times(1)).save(txToSave)

        verify(blockTxRepository, times(1)).save(CqlBitcoinBlockTxPreview(txABlock))

        verify(contractTxRepository, times(1)).saveAll(
            txABlock.allContractsUsedInTransaction().map { it ->
                CqlBitcoinContractTxPreview(it, txABlock)
            }
        )
        verify(contractTxRepository, times(1)).deleteAll(
            txABlock.allContractsUsedInTransaction().map { it ->
                CqlBitcoinContractTxPreview(it, txABlock.mempoolState())
            }
        )
    }

    @Test
    fun testNewBlockWithTxInDb() {
        val txADb = tx("A", "A").mempoolState().copy(firstSeenTime = Instant.ofEpochMilli(999999))
        val txABlock = tx("A", "A")

        val txToSave = CqlBitcoinTx(txABlock.copy(firstSeenTime = txADb.firstSeenTime))

        val txRepository = mock<BitcoinTxRepository> {
            on { save(any<CqlBitcoinTx>()) }.thenReturn(Mono.just(txToSave))
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.just(CqlBitcoinTx(txADb)))
        }
        val blockTxRepository = mock<BitcoinBlockTxRepository> {
            on { save(any<CqlBitcoinBlockTxPreview>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractTxRepository = mock<BitcoinContractTxRepository> {
            on { saveAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.NEW_BLOCK, txABlock)

        val txDumpProcess = TxDumpProcess(txRepository, contractTxRepository, blockTxRepository,
            BitcoinFamilyChain.BITCOIN)

        txDumpProcess.onMessage(listOf(record))



        verify(txRepository, times(1)).save(txToSave)
        verify(txRepository, never()).save(CqlBitcoinTx(txABlock))

        verify(blockTxRepository, times(1)).save(CqlBitcoinBlockTxPreview(txABlock))

        verify(contractTxRepository, times(1)).saveAll(
            txABlock.allContractsUsedInTransaction().map { it ->
                CqlBitcoinContractTxPreview(it, txABlock)
            }
        )
        verify(contractTxRepository, times(1)).deleteAll(
            txABlock.allContractsUsedInTransaction().map { it ->
                CqlBitcoinContractTxPreview(it, txABlock.mempoolState())
            }
        )
    }

    @Test
    fun testDropBlockWithoutTxInDb() {
        val txADrop = tx("A", "A")

        val txToSave = CqlBitcoinTx(txADrop.mempoolState())

        val txRepository = mock<BitcoinTxRepository> {
            on { save(any<CqlBitcoinTx>()) }.thenReturn(Mono.just(txToSave))
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.empty())
        }
        val blockTxRepository = mock<BitcoinBlockTxRepository> {
            on { save(any<CqlBitcoinBlockTxPreview>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractTxRepository = mock<BitcoinContractTxRepository> {
            on { saveAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.DROPPED_BLOCK, txADrop)

        val txDumpProcess = TxDumpProcess(txRepository, contractTxRepository, blockTxRepository,
            BitcoinFamilyChain.BITCOIN)

        txDumpProcess.onMessage(listOf(record))

        verify(txRepository, times(1)).save(txToSave)

        verify(blockTxRepository, times(1)).delete(CqlBitcoinBlockTxPreview(txADrop))

        verify(contractTxRepository, times(1)).deleteAll(
            txADrop.allContractsUsedInTransaction().map { it ->
                CqlBitcoinContractTxPreview(it, txADrop)
            }
        )
    }

    @Test
    fun testDropBlockWithTxInDb() {
        val txADb = tx("A", "A").copy(firstSeenTime = Instant.ofEpochMilli(999999))
        val txADrop = tx("A", "B")

        val txToSave = CqlBitcoinTx(txADrop.mempoolState().copy(firstSeenTime = txADb.firstSeenTime))

        val txRepository = mock<BitcoinTxRepository> {
            on { save(any<CqlBitcoinTx>()) }.thenReturn(Mono.just(txToSave))
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.just(CqlBitcoinTx(txADb)))
        }
        val blockTxRepository = mock<BitcoinBlockTxRepository> {
            on { save(any<CqlBitcoinBlockTxPreview>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractTxRepository = mock<BitcoinContractTxRepository> {
            on { saveAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.DROPPED_BLOCK, txADrop)

        val txDumpProcess = TxDumpProcess(txRepository, contractTxRepository, blockTxRepository,
            BitcoinFamilyChain.BITCOIN)

        txDumpProcess.onMessage(listOf(record))

        verify(txRepository, times(1)).save(txToSave)
        verify(txRepository, never()).save(CqlBitcoinTx(txADrop.mempoolState()))

        verify(blockTxRepository, times(1)).delete(CqlBitcoinBlockTxPreview(txADrop))

        verify(contractTxRepository, times(1)).deleteAll(
            txADrop.allContractsUsedInTransaction().map { it ->
                CqlBitcoinContractTxPreview(it, txADrop)
            }
        )
    }

    @Test
    fun testNewPoolWithoutTxInDb() {
        val txAPool = tx("A", "A").mempoolState()

        val txToSave = CqlBitcoinTx(txAPool)
        val contractTxesToSave = txAPool.allContractsUsedInTransaction().map { it ->
            CqlBitcoinContractTxPreview(it, txAPool)
        }

        val txRepository = mock<BitcoinTxRepository> {
            on { save(any<CqlBitcoinTx>()) }.thenReturn(Mono.just(txToSave))
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.empty())
        }
        val blockTxRepository = mock<BitcoinBlockTxRepository>()

        val contractTxRepository = mock<BitcoinContractTxRepository> {
            on { saveAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }
                .thenReturn(Flux.fromIterable(contractTxesToSave))
            on { deleteAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.NEW_POOL_TX, txAPool)

        val txDumpProcess = TxDumpProcess(txRepository, contractTxRepository, blockTxRepository,
            BitcoinFamilyChain.BITCOIN)

        txDumpProcess.onMessage(listOf(record))

        verify(txRepository, times(1)).save(txToSave)

        verify(contractTxRepository, times(1)).saveAll(contractTxesToSave)
    }

    @Test
    fun testNewPoolWithTxInDb() {
        val txADb = tx("A", "A")
        val txAPool = tx("A", "A").mempoolState()

        val txRepository = mock<BitcoinTxRepository> {
            on { save(any<CqlBitcoinTx>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.just(CqlBitcoinTx(txADb)))
        }
        val blockTxRepository = mock<BitcoinBlockTxRepository>()

        val contractTxRepository = mock<BitcoinContractTxRepository> {
            on { saveAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlBitcoinContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.NEW_POOL_TX, txAPool)

        val txDumpProcess = TxDumpProcess(txRepository, contractTxRepository, blockTxRepository,
            BitcoinFamilyChain.BITCOIN)

        txDumpProcess.onMessage(listOf(record))

        verify(txRepository, never()).save(any<CqlBitcoinTx>())

        verify(contractTxRepository, never()).saveAll(any<Iterable<CqlBitcoinContractTxPreview>>())
    }

    fun tx(hash: String, blockHash: String?) = BitcoinTx(
        hash = hash, blockHash = blockHash, blockNumber = 4959189, blockTime = Instant.ofEpochSecond(100000),
        index = 1, firstSeenTime = Instant.ofEpochSecond(100000),
        fee = BigDecimal.ZERO, size = 1, totalOutputsAmount = BigDecimal.ONE, totalInputsAmount = BigDecimal.ONE,
        ins = listOf(BitcoinTxIn(listOf("a"), BigDecimal.ONE, SignatureScript("a", "a"), emptyList(), "a0", 0)),
        outs = listOf(BitcoinTxOut(listOf("b"), BigDecimal.ONE, "a", 0, 1))
    )

    fun record(event: PumpEvent, tx: BitcoinTx) =
        ConsumerRecord<PumpEvent, BitcoinTx>(BitcoinFamilyChain.BITCOIN.txPumpTopic, 0, 0, event, tx)

}
