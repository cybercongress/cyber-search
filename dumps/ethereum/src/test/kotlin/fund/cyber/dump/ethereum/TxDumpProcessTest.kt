package fund.cyber.dump.ethereum

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Instant

class TxDumpProcessTest {


    //        --- D --- E --- G --- I
    //A --- B --- C --- F --- H
    @Test
    @Suppress("LongMethod")
    fun testWithDroppedTxs() {

        val txC = EthereumTx(
                hash = "C", error = null,
                nonce = 0, blockHash = "C",
                blockNumber = 4959189, blockTime = Instant.ofEpochSecond(100000), positionInBlock = 1,
                from = "a", to = "b", firstSeenTime = Instant.ofEpochSecond(100000),
                value = BigDecimal.ZERO, gasPrice = BigDecimal.ZERO, gasLimit = 0,
                gasUsed = 21000L, fee = BigDecimal.ZERO, input = "", createdSmartContract = null, trace = null
        )

        val txD = EthereumTx(
                hash = "D", error = null,
                nonce = 0, blockHash = "D",
                blockNumber = 4959189, blockTime = Instant.ofEpochSecond(100000), positionInBlock = 1,
                from = "a", to = "b", firstSeenTime = Instant.ofEpochSecond(100000),
                value = BigDecimal.ZERO, gasPrice = BigDecimal.ZERO, gasLimit = 0,
                gasUsed = 21000L, fee = BigDecimal.ZERO, input = "", createdSmartContract = null, trace = null
        )

        val txE = EthereumTx(
                hash = "E", error = null,
                nonce = 0, blockHash = "E",
                blockNumber = 4959189, blockTime = Instant.ofEpochSecond(100000), positionInBlock = 1,
                from = "a", to = "b", firstSeenTime = Instant.ofEpochSecond(100000),
                value = BigDecimal.ZERO, gasPrice = BigDecimal.ZERO, gasLimit = 0,
                gasUsed = 21000L, fee = BigDecimal.ZERO, input = "", createdSmartContract = null, trace = null
        )

        val txF = EthereumTx(
                hash = "F", error = null,
                nonce = 0, blockHash = "F",
                blockNumber = 4959189, blockTime = Instant.ofEpochSecond(100000), positionInBlock = 1,
                from = "a", to = "b", firstSeenTime = Instant.ofEpochSecond(100000),
                value = BigDecimal.ZERO, gasPrice = BigDecimal.ZERO, gasLimit = 0,
                gasUsed = 21000L, fee = BigDecimal.ZERO, input = "", createdSmartContract = null, trace = null
        )

        val txG = EthereumTx(
                hash = "G", error = null,
                nonce = 0, blockHash = "G",
                blockNumber = 4959189, blockTime = Instant.ofEpochSecond(100000), positionInBlock = 1,
                from = "a", to = "b", firstSeenTime = Instant.ofEpochSecond(100000),
                value = BigDecimal.ZERO, gasPrice = BigDecimal.ZERO, gasLimit = 0,
                gasUsed = 21000L, fee = BigDecimal.ZERO, input = "", createdSmartContract = null, trace = null
        )

        val txH = EthereumTx(
                hash = "H", error = null,
                nonce = 0, blockHash = "H",
                blockNumber = 4959189, blockTime = Instant.ofEpochSecond(100000), positionInBlock = 1,
                from = "a", to = "b", firstSeenTime = Instant.ofEpochSecond(100000),
                value = BigDecimal.ZERO, gasPrice = BigDecimal.ZERO, gasLimit = 0,
                gasUsed = 21000L, fee = BigDecimal.ZERO, input = "", createdSmartContract = null, trace = null
        )

        val txI = EthereumTx(
                hash = "I", error = null,
                nonce = 0, blockHash = "I",
                blockNumber = 4959189, blockTime = Instant.ofEpochSecond(100000), positionInBlock = 1,
                from = "a", to = "b", firstSeenTime = Instant.ofEpochSecond(100000),
                value = BigDecimal.ZERO, gasPrice = BigDecimal.ZERO, gasLimit = 0,
                gasUsed = 21000L, fee = BigDecimal.ZERO, input = "", createdSmartContract = null, trace = null
        )

        val txK = EthereumTx(
                hash = "K", error = null,
                nonce = 0, blockHash = null,
                blockNumber = -1, blockTime = null, positionInBlock = -1,
                from = "a", to = "b", firstSeenTime = Instant.ofEpochSecond(100000),
                value = BigDecimal.ZERO, gasPrice = BigDecimal.ZERO, gasLimit = 0,
                gasUsed = 21000L, fee = BigDecimal.ZERO, input = "", createdSmartContract = null, trace = null
        )


        val record1 = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, txH)
        val record2 = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0,
                0, PumpEvent.DROPPED_BLOCK, txH)
        val record3 = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0,
                0, PumpEvent.DROPPED_BLOCK, txF)
        val record4 = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0,
                0, PumpEvent.DROPPED_BLOCK, txC)
        val record5 = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, txD)
        val record6 = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, txE)
        val record7 = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, txG)
        val record8 = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0,
                0, PumpEvent.NEW_BLOCK, txI)
        val record9 = ConsumerRecord<PumpEvent, EthereumTx>(EthereumFamilyChain.ETHEREUM.txPumpTopic, 0,
            0, PumpEvent.NEW_POOL_TX, txK)

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

        val txDumpProcess = TxDumpProcess(txRepository, blockTxRepository, contractTxRepository,
                EthereumFamilyChain.ETHEREUM)

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


}
