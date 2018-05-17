package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumBlockTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.dump.common.filterNotContainsAllEventsOf
import fund.cyber.dump.common.toRecordEventsMap
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.BatchMessageListener


//todo in dump add label for smart contract from calls
/**
 * Transform transaction based on event. For [PumpEvent.DROPPED_BLOCK] we transform transaction to mempool state.
 * For others it remains as it is.
 *
 * @param event used to transform
 * @return transformed transaction
 */
fun EthereumTx.transformBy(event: PumpEvent) = if (event == PumpEvent.DROPPED_BLOCK) this.mempoolState() else this

class TxDumpProcess(
    private val txRepository: EthereumTxRepository,
    private val blockTxRepository: EthereumBlockTxRepository,
    private val contractTxRepository: EthereumContractTxRepository,
    private val chain: EthereumFamilyChain
) : BatchMessageListener<PumpEvent, EthereumTx> {

    private val log = LoggerFactory.getLogger(BatchMessageListener::class.java)

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, EthereumTx>>) {

        log.info("Dumping batch of ${records.size} $chain transactions from offset ${records.first().offset()}")

        val currentDbTxs = txRepository
            .findAllById(records.map { r -> r.value().hash }).collectList().block()!!
            .associateBy { tx -> tx.hash }

        saveTransactions(records, currentDbTxs)
        saveBlockTransactions(records)
        saveContractTransactions(records, currentDbTxs)
    }

    /**
     * Save transactions to cassandra. We assume that all events are sequential.
     * If in batch we have a lot of events for same transaction hash then we take the last one event
     * and saving its state in database.
     *
     * [PumpEvent.DROPPED_BLOCK] returning transaction in mempool state.
     *
     * For [PumpEvent.NEW_POOL_TX] event if we already have this transaction in database we skipping this event
     * cause it means that chain reorganization occurred and it's already (or will be) returned in mempool state by
     * [PumpEvent.DROPPED_BLOCK] event.
     *
     * [EthereumTx.firstSeenTime] for transactions is updated from database every time if we already
     * have this transaction in database.
     *
     * @param records kafka records with pairs of events and transactions to save.
     * @param currentDbTxs map of transactions by hash that are already in database.
     */
    private fun saveTransactions(records: List<ConsumerRecord<PumpEvent, EthereumTx>>,
                                 currentDbTxs: Map<String, CqlEthereumTx>): Map<String, CqlEthereumTx> {
        val txLastEventMap = recordIdToLastEvent(records)

        val txsToSave = txLastEventMap
            // we don't need to save pool tx if we already have some state for this tx in DB
            .filter { entry -> !(entry.value.first == PumpEvent.NEW_POOL_TX && currentDbTxs.contains(entry.key)) }
            .map { entry ->
                val (event, tx) = entry.value
                val currentDbTx = currentDbTxs[entry.value.second.hash]
                val newTx = tx.transformBy(event)
                // Set first seen time from previous state in DB
                if (currentDbTx != null) {
                    return@map newTx.copy(firstSeenTime = currentDbTx.firstSeenTime)
                }
                return@map newTx
            }

        log.info("Total transactions to save ${txsToSave.size}")

        txRepository.saveAll(txsToSave.map { tx -> CqlEthereumTx(tx) }).collectList().block()
        return currentDbTxs
    }

    /**
     * Save transactions previews by block to cassandra.
     * Here we don't need to handle transaction with [PumpEvent.NEW_POOL_TX] event cause they're not in block.
     *
     * Preview is unique by [CqlEthereumBlockTxPreview.blockNumber] and [CqlEthereumBlockTxPreview.positionInBlock]
     * so if we have [PumpEvent.NEW_BLOCK] and [PumpEvent.DROPPED_BLOCK] events at the same time for transactions
     * with the same [EthereumTx.blockNumber] and [EthereumTx.positionInBlock] then we don't need to do anything on it.
     *
     * @param records kafka records with pairs of events and transactions to save.
     */
    private fun saveBlockTransactions(records: List<ConsumerRecord<PumpEvent, EthereumTx>>) {
        // we don't need to handle NEW_POOL_TXes for transactions by block preview.
        val blockTxsToProcess = records
            .filter { record -> record.key() != PumpEvent.NEW_POOL_TX }
            .toRecordEventsMap()
            .filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK))
            .map { entry -> CqlEthereumBlockTxPreview(entry.key) to entry.value }
            .toMap()

        blockTxRepository
            .deleteAll(blockTxsToProcess.filter { entry -> entry.value.contains(PumpEvent.DROPPED_BLOCK) }.keys)
            .block()
        blockTxRepository
            .saveAll(blockTxsToProcess.filter { entry -> entry.value.contains(PumpEvent.NEW_BLOCK) }.keys)
            .collectList().block()
    }

    /**
     * Save transactions previews by contract to cassandra.
     * Firstly we saving transactions with [PumpEvent.NEW_POOL_TX] event. It's neccesary cause we could have
     * [PumpEvent.NEW_POOL_TX] event and [PumpEvent.NEW_BLOCK] event at the same batch. By saving
     * [PumpEvent.NEW_POOL_TX] first we could then delete it with other mempool transactions that should be deleted
     * by [PumpEvent.NEW_BLOCK] event that means transaction is not in mempool anymore.
     *
     * @param records kafka records with pairs of events and transactions to save.
     * @param currentDbTxs map of transactions by hash that are already in database.
     */
    private fun saveContractTransactions(records: List<ConsumerRecord<PumpEvent, EthereumTx>>,
                                         currentDbTxs: Map<String, CqlEthereumTx>) {
        val contractTxsToProcess = records
            .toRecordEventsMap()
            .filterNotContainsAllEventsOf(listOf(PumpEvent.NEW_BLOCK, PumpEvent.DROPPED_BLOCK))
            .map { entry ->
                val currentDbTx = currentDbTxs[entry.key.hash]
                val newTx = entry.key
                // Set first seen time from previous state in DB
                if (currentDbTx != null) {
                    return@map newTx.copy(firstSeenTime = currentDbTx.firstSeenTime) to entry.value
                }
                return@map newTx to entry.value
            }.toMap()


        fun Collection<EthereumTx>.toContractTxes() = this.flatMap { tx ->
            tx.contractsUsedInTransaction().map { it -> CqlEthereumContractTxPreview(tx, it) }
        }

        val mempoolContractTxesToSave = contractTxsToProcess
            .filter { entry -> entry.value.contains(PumpEvent.NEW_POOL_TX) }
            .keys.toContractTxes()
        // We should drop all txes with DROPPED_BLOCK event and mempool state transactions for NEW_BLOCK txes
        val contractTxesToDrop = contractTxsToProcess.filter { entry -> !entry.value.contains(PumpEvent.NEW_POOL_TX) }
            .map { entry -> if (entry.value.contains(PumpEvent.NEW_BLOCK)) entry.key.mempoolState() else entry.key }
            .toContractTxes()
        val contractTxesToSave = contractTxsToProcess
            .filter { entry -> entry.value.contains(PumpEvent.NEW_BLOCK) }
            .keys.toContractTxes()


        contractTxRepository.saveAll(mempoolContractTxesToSave).collectList().block()
        contractTxRepository.deleteAll(contractTxesToDrop).block()
        contractTxRepository.saveAll(contractTxesToSave).collectList().block()
    }

    /**
     * Create map of record id to last applied event and state to handle situation when we have multiply records
     * in batch with the same id. It could happen when they have different events.
     *
     * @param records list of records
     * @return map of record id to last applied event and record state
     *
     */
    private fun recordIdToLastEvent(
        records: List<ConsumerRecord<PumpEvent, EthereumTx>>
    ): MutableMap<String, Pair<PumpEvent, EthereumTx>> {
        val txLastEventMap = mutableMapOf<String, Pair<PumpEvent, EthereumTx>>()
        records.forEach { record ->
            val event = record.key()
            val tx = record.value()
            val currentTxState = txLastEventMap[tx.hash]
            if (currentTxState == null) {
                txLastEventMap[tx.hash] = event to tx
            } else {
                if (record.key() != PumpEvent.NEW_POOL_TX) {
                    txLastEventMap[tx.hash] = event to tx
                }
            }
        }
        return txLastEventMap
    }

}
