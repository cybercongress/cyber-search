package fund.cyber.pump

import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.common.Chain.BITCOIN_CASH
import fund.cyber.node.common.StackCache
import fund.cyber.node.model.IndexingProgress
import fund.cyber.pump.bitcoin.BitcoinBlockchainInterface
import fund.cyber.pump.bitcoin_cash.BitcoinCashBlockchainInterface
import fund.cyber.pump.cassandra.CassandraStorage
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import java.util.*


private val log = LoggerFactory.getLogger(PumpsApplication::class.java)!!


object PumpsApplication {

    @JvmStatic
    fun main(args: Array<String>) {

        val blockchainsInterfaces = PumpsConfiguration.chainsToPump.mapNotNull(::initializeBlockchainInterface)
        val storages: List<StorageInterface> = listOf(CassandraStorage())

        storages.forEach { storage ->
            storage.initialize(blockchainsInterfaces)
        }

        blockchainsInterfaces.forEach { blockchainInterface ->

            val startBlockNumber = getCommittedBlockNumber(blockchainInterface)
            val history: StackCache<List<StorageAction>> = StackCache(20)

            log.info("${blockchainInterface.chain} chain pump is started from `${startBlockNumber + 1}` block")
            initBlockBundleStream(blockchainInterface, storages, startBlockNumber, history)
        }

        if (blockchainsInterfaces.isEmpty()) {
            PumpsContext.closeContext()
        }
    }
}


fun initBlockBundleStream(
        blockchainInterface: BlockchainInterface<*>, storages: List<StorageInterface>,
        startBlockNumber: Long, history: StackCache<List<StorageAction>>) {

    blockchainInterface.subscribeBlocks(startBlockNumber)
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.io())
            .map { bundle -> bundle } // make generics happy
            .scan { current, next ->
                if (current.hash != next.parentHash) {
                    log.info("${current.chain} chain reorganisation occupied for `${current.number}` block")
                    //history.pop()?.forEach(StorageAction::remove) bug in pop
                    initBlockBundleStream(blockchainInterface, storages, current.number - 1, history)
                    throw ChainReindexationException()
                }
                next
            }
            .skipWhile { bundle -> bundle.number == startBlockNumber && startBlockNumber != 0L }
            .subscribe(
                    { blockBundle ->
                        log.debug("Storing ${blockBundle.chain} ${blockBundle.number} block")
                        val actions = storages.map { it.constructAction(blockBundle) }
                        actions.forEach { it.store() }

                        history.push(actions)
                        commitBlockPumpIndexation(blockBundle)
                    },
                    { error ->
                        if (error !is ChainReindexationException)
                            log.error("Error during processing ${blockchainInterface.chain} stream", error)

                    }
            )
}


private fun commitBlockPumpIndexation(blockBundle: BlockBundle) {

    log.debug("Commit ${blockBundle.chain} ${blockBundle.number} block indexation")
    val progress = IndexingProgress(
            application_id = chainApplicationId(blockBundle.chain),
            block_number = blockBundle.number, block_hash = blockBundle.hash, index_time = Date()
    )

    PumpsContext.pumpDaoService.indexingProgressStore.save(progress)
}


private fun getCommittedBlockNumber(blockchainInterface: BlockchainInterface<*>): Long {

    val applicationId = chainApplicationId(blockchainInterface.chain)

    return if (PumpsConfiguration.startBlock == CS_START_BLOCK_DEFAULT)
        PumpsContext.pumpDaoService.indexingProgressStore.get(applicationId)?.block_number ?: 0
    else
        PumpsConfiguration.startBlock
}

private fun initializeBlockchainInterface(chain: Chain): BlockchainInterface<*>? {
    return when (chain) {
        BITCOIN -> BitcoinBlockchainInterface()
        BITCOIN_CASH -> BitcoinCashBlockchainInterface()
        else -> null
    }
}