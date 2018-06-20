package fund.cyber.dump.ethereum

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.never
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumBlockTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import io.micrometer.core.instrument.Timer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Instant

@Suppress("LargeClass")
class TxDumpProcessTest {


    private val chainInfo = ChainInfo(ChainFamily.ETHEREUM)

    private val txLatencyMetric = mock<Timer>()


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

        val txRepository = mock<EthereumTxRepository> {
            on { save(any<CqlEthereumTx>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.empty())
        }
        val blockTxRepository = mock<EthereumBlockTxRepository> {
            on { save(any<CqlEthereumBlockTxPreview>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractTxRepository = mock<EthereumContractTxRepository> {
            on { saveAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val txDumpProcess = TxDumpProcess(txRepository, blockTxRepository, contractTxRepository, chainInfo, 0, txLatencyMetric)

        txDumpProcess.onMessage(listOf(record1, record2, record3, record4, record5, record6, record7, record8, record9))


        verify(txRepository, times(2)).findById(txH.hash)
        verify(txRepository, times(1)).findById(txF.hash)
        verify(txRepository, times(1)).findById(txC.hash)
        verify(txRepository, times(1)).findById(txD.hash)
        verify(txRepository, times(1)).findById(txE.hash)
        verify(txRepository, times(1)).findById(txG.hash)
        verify(txRepository, times(1)).findById(txI.hash)
        verify(txRepository, times(1)).findById(txK.hash)

        verify(txRepository, times(1)).save(CqlEthereumTx(txH))
        verify(txRepository, times(1)).save(CqlEthereumTx(txH.mempoolState()))
        verify(txRepository, times(1)).save(CqlEthereumTx(txF.mempoolState()))
        verify(txRepository, times(1)).save(CqlEthereumTx(txC.mempoolState()))
        verify(txRepository, times(1)).save(CqlEthereumTx(txD))
        verify(txRepository, times(1)).save(CqlEthereumTx(txE))
        verify(txRepository, times(1)).save(CqlEthereumTx(txG))
        verify(txRepository, times(1)).save(CqlEthereumTx(txI))
        verify(txRepository, times(1)).save(CqlEthereumTx(txK))

        verify(blockTxRepository, times(1)).save(CqlEthereumBlockTxPreview(txH))
        verify(blockTxRepository, times(1)).delete(CqlEthereumBlockTxPreview(txH))
        verify(blockTxRepository, times(1)).delete(CqlEthereumBlockTxPreview(txF))
        verify(blockTxRepository, times(1)).delete(CqlEthereumBlockTxPreview(txC))
        verify(blockTxRepository, times(1)).save(CqlEthereumBlockTxPreview(txD))
        verify(blockTxRepository, times(1)).save(CqlEthereumBlockTxPreview(txE))
        verify(blockTxRepository, times(1)).save(CqlEthereumBlockTxPreview(txG))
        verify(blockTxRepository, times(1)).save(CqlEthereumBlockTxPreview(txI))


        listOf(txH, txF, txC, txH.mempoolState(), txD.mempoolState(), txE.mempoolState(), txG.mempoolState(), txI.mempoolState())
            .forEach { tx ->
                verify(contractTxRepository, times(1))
                    .deleteAll(tx.contractsUsedInTransaction().map { it -> CqlEthereumContractTxPreview(tx, it) })
            }

        listOf(txH, txD, txE, txG, txI, txK)
            .forEach { tx ->
                verify(contractTxRepository, times(1))
                    .saveAll(tx.contractsUsedInTransaction().map { it -> CqlEthereumContractTxPreview(tx, it) })
            }
    }

    @Test
    fun testNewBlockWithoutTxInDb() {
        val txABlock = tx("A", "A")

        val txToSave = CqlEthereumTx(txABlock)

        val txRepository = mock<EthereumTxRepository> {
            on { save(any<CqlEthereumTx>()) }.thenReturn(Mono.just(txToSave))
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.empty())
        }
        val blockTxRepository = mock<EthereumBlockTxRepository> {
            on { save(any<CqlEthereumBlockTxPreview>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractTxRepository = mock<EthereumContractTxRepository> {
            on { saveAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.NEW_BLOCK, txABlock)

        val txDumpProcess = TxDumpProcess(txRepository, blockTxRepository, contractTxRepository, chainInfo, 0, txLatencyMetric)

        txDumpProcess.onMessage(listOf(record))

        verify(txRepository, times(1)).save(txToSave)

        verify(blockTxRepository, times(1)).save(CqlEthereumBlockTxPreview(txABlock))

        verify(contractTxRepository, times(1)).saveAll(
            txABlock.contractsUsedInTransaction().map { it ->
                CqlEthereumContractTxPreview(txABlock, it)
            }
        )
        verify(contractTxRepository, times(1)).deleteAll(
            txABlock.contractsUsedInTransaction().map { it ->
                CqlEthereumContractTxPreview(txABlock.mempoolState(), it)
            }
        )
    }

    @Test
    fun testNewBlockWithTxInDb() {
        val txADb = tx("A", "A").mempoolState().copy(firstSeenTime = Instant.ofEpochMilli(999999))
        val txABlock = tx("A", "A")

        val txToSave = CqlEthereumTx(txABlock.copy(firstSeenTime = txADb.firstSeenTime))

        val txRepository = mock<EthereumTxRepository> {
            on { save(any<CqlEthereumTx>()) }.thenReturn(Mono.just(txToSave))
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.just(CqlEthereumTx(txADb)))
        }
        val blockTxRepository = mock<EthereumBlockTxRepository> {
            on { save(any<CqlEthereumBlockTxPreview>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractTxRepository = mock<EthereumContractTxRepository> {
            on { saveAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.NEW_BLOCK, txABlock)

        val txDumpProcess = TxDumpProcess(txRepository, blockTxRepository, contractTxRepository, chainInfo, 0, txLatencyMetric)

        txDumpProcess.onMessage(listOf(record))



        verify(txRepository, times(1)).save(txToSave)
        verify(txRepository, never()).save(CqlEthereumTx(txABlock))

        verify(blockTxRepository, times(1)).save(CqlEthereumBlockTxPreview(txABlock))

        verify(contractTxRepository, times(1)).saveAll(
            txABlock.contractsUsedInTransaction().map { it ->
                CqlEthereumContractTxPreview(txABlock, it)
            }
        )
        verify(contractTxRepository, times(1)).deleteAll(
            txABlock.contractsUsedInTransaction().map { it ->
                CqlEthereumContractTxPreview(txABlock.mempoolState(), it)
            }
        )
    }

    @Test
    fun testDropBlockWithoutTxInDb() {
        val txADrop = tx("A", "A")

        val txToSave = CqlEthereumTx(txADrop.mempoolState())

        val txRepository = mock<EthereumTxRepository> {
            on { save(any<CqlEthereumTx>()) }.thenReturn(Mono.just(txToSave))
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.empty())
        }
        val blockTxRepository = mock<EthereumBlockTxRepository> {
            on { save(any<CqlEthereumBlockTxPreview>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractTxRepository = mock<EthereumContractTxRepository> {
            on { saveAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.DROPPED_BLOCK, txADrop)

        val txDumpProcess = TxDumpProcess(txRepository, blockTxRepository, contractTxRepository, chainInfo, 0, txLatencyMetric)

        txDumpProcess.onMessage(listOf(record))

        verify(txRepository, times(1)).save(txToSave)

        verify(blockTxRepository, times(1)).delete(CqlEthereumBlockTxPreview(txADrop))

        verify(contractTxRepository, times(1)).deleteAll(
            txADrop.contractsUsedInTransaction().map { it ->
                CqlEthereumContractTxPreview(txADrop, it)
            }
        )
    }

    @Test
    fun testDropBlockWithTxInDb() {
        val txADb = tx("A", "A").copy(firstSeenTime = Instant.ofEpochMilli(999999))
        val txADrop = tx("A", "B")

        val txToSave = CqlEthereumTx(txADrop.mempoolState().copy(firstSeenTime = txADb.firstSeenTime))

        val txRepository = mock<EthereumTxRepository> {
            on { save(any<CqlEthereumTx>()) }.thenReturn(Mono.just(txToSave))
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.just(CqlEthereumTx(txADb)))
        }
        val blockTxRepository = mock<EthereumBlockTxRepository> {
            on { save(any<CqlEthereumBlockTxPreview>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
        }
        val contractTxRepository = mock<EthereumContractTxRepository> {
            on { saveAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.DROPPED_BLOCK, txADrop)

        val txDumpProcess = TxDumpProcess(txRepository, blockTxRepository, contractTxRepository, chainInfo, 0, txLatencyMetric)

        txDumpProcess.onMessage(listOf(record))

        verify(txRepository, times(1)).save(txToSave)
        verify(txRepository, never()).save(CqlEthereumTx(txADrop.mempoolState()))

        verify(blockTxRepository, times(1)).delete(CqlEthereumBlockTxPreview(txADrop))

        verify(contractTxRepository, times(1)).deleteAll(
            txADrop.contractsUsedInTransaction().map { it ->
                CqlEthereumContractTxPreview(txADrop, it)
            }
        )
    }

    @Test
    fun testNewPoolWithoutTxInDb() {
        val txAPool = tx("A", "A").mempoolState()

        val txToSave = CqlEthereumTx(txAPool)
        val contractTxesToSave = txAPool.contractsUsedInTransaction().map { it ->
            CqlEthereumContractTxPreview(txAPool, it)
        }

        val txRepository = mock<EthereumTxRepository> {
            on { save(any<CqlEthereumTx>()) }.thenReturn(Mono.just(txToSave))
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.empty())
        }
        val blockTxRepository = mock<EthereumBlockTxRepository>()

        val contractTxRepository = mock<EthereumContractTxRepository> {
            on { saveAll(any<Iterable<CqlEthereumContractTxPreview>>()) }
                .thenReturn(Flux.fromIterable(contractTxesToSave))
            on { deleteAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.NEW_POOL_TX, txAPool)

        val txDumpProcess = TxDumpProcess(txRepository, blockTxRepository, contractTxRepository, chainInfo, 0, txLatencyMetric)

        txDumpProcess.onMessage(listOf(record))

        verify(txRepository, times(1)).save(txToSave)

        verify(contractTxRepository, times(1)).saveAll(contractTxesToSave)
    }

    @Test
    fun testNewPoolWithTxInDb() {
        val txADb = tx("A", "A")
        val txAPool = tx("A", "A").mempoolState()

        val txRepository = mock<EthereumTxRepository> {
            on { save(any<CqlEthereumTx>()) }.thenReturn(Mono.empty())
            on { delete(any()) }.thenReturn(Mono.empty())
            on { findById(any<String>()) }.thenReturn(Mono.just(CqlEthereumTx(txADb)))
        }
        val blockTxRepository = mock<EthereumBlockTxRepository>()

        val contractTxRepository = mock<EthereumContractTxRepository> {
            on { saveAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Flux.empty())
            on { deleteAll(any<Iterable<CqlEthereumContractTxPreview>>()) }.thenReturn(Mono.empty())
        }

        val record = record(PumpEvent.NEW_POOL_TX, txAPool)

        val txDumpProcess = TxDumpProcess(txRepository, blockTxRepository, contractTxRepository, chainInfo, 0, txLatencyMetric)

        txDumpProcess.onMessage(listOf(record))

        verify(txRepository, never()).save(any<CqlEthereumTx>())

        verify(contractTxRepository, never()).saveAll(any<Iterable<CqlEthereumContractTxPreview>>())
    }

    fun tx(hash: String, blockHash: String?) = EthereumTx(
        hash = hash, error = null, nonce = 0, blockHash = blockHash,
        blockNumber = 4959189, blockTime = Instant.ofEpochSecond(100000), positionInBlock = 1,
        from = "a", to = "b", firstSeenTime = Instant.ofEpochSecond(100000),
        value = BigDecimal.ZERO, gasPrice = BigDecimal.ZERO, gasLimit = 0,
        gasUsed = 21000L, fee = BigDecimal.ZERO, input = "", createdSmartContract = null, trace = null
    )

    fun record(event: PumpEvent, tx: EthereumTx) =
        ConsumerRecord<PumpEvent, EthereumTx>(chainInfo.txPumpTopic, 0, 0, event, tx)

}
