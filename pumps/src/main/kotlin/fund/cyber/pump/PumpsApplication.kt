package fund.cyber.pump

import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.*
import fund.cyber.node.common.StackCache
import fund.cyber.node.common.env
import fund.cyber.node.model.IndexingProgress
import fund.cyber.pump.bitcoin.BitcoinBlockchainInterface
import fund.cyber.pump.bitcoin_cash.BitcoinCashBlockchainInterface
import fund.cyber.pump.cassandra.CassandraStorage
import fund.cyber.pump.ethereum.EthereumBlockchainInterface
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import java.util.*


private val log = LoggerFactory.getLogger(PumpsApplication::class.java)!!


object PumpsApplication {

    @JvmStatic
    fun main(args: Array<String>) {

        val blockchains = PumpsConfiguration.chainsToPump.mapNotNull(::initializeBlockchainInterface)
        val storages: List<StorageInterface> = listOf(CassandraStorage())

        storages.forEach { storage ->
            storage.initialize(blockchains)
        }

        blockchains.forEach { blockchain ->

            val startBlockNumber = getCommittedBlockNumber(blockchain)
            val history: StackCache<List<StorageAction>> = StackCache(20)

            log.info("${blockchain.chain} chain pump is started from `${startBlockNumber + 1}` block")
            initBlockBundleStream(blockchain, storages, startBlockNumber, history)
        }

        if (blockchains.isEmpty()) {
            PumpsContext.closeContext()
        }
    }
}


fun initBlockBundleStream(
        blockchainInterface: FlowableBlockchain, storages: List<StorageInterface>,
        startBlockNumber: Long, history: StackCache<List<StorageAction>>) {

    blockchainInterface.subscribeBlocks(startBlockNumber)
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.io())
            .map { bundle -> bundle } // make generics happy
            .scan { current, next ->
                if (current.hash != next.parentHash) {
                    log.info("${current.chain} chain reorganisation occupied for `${current.number}` block")
                    history.pop()?.forEach(StorageAction::remove)
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


private fun getCommittedBlockNumber(blockchainInterface: Blockchain): Long {

    val applicationId = chainApplicationId(blockchainInterface.chain)

    return if (PumpsConfiguration.startBlock == CS_START_BLOCK_DEFAULT)
        PumpsContext.pumpDaoService.indexingProgressStore.get(applicationId)?.block_number ?: 0
    else
        PumpsConfiguration.startBlock
}

private fun initializeBlockchainInterface(chain: Chain): FlowableBlockchain? {
    return when (chain) {
        BITCOIN -> BitcoinBlockchainInterface()
        BITCOIN_CASH -> BitcoinCashBlockchainInterface()
        ETHEREUM -> SerialPulledBlockhain(EthereumBlockchainInterface(env("ETHEREUM", "http://cyber:cyber@127.0.0.1:8545"), ETHEREUM))
        ETHEREUM_CLASSIC -> SerialPulledBlockhain(EthereumBlockchainInterface(env("ETHEREUM_CLASSIC", "http://cyber:cyber@127.0.0.1:18545"), ETHEREUM_CLASSIC))
        else -> null
    }
}

class ChainReindexationException : RuntimeException()