package fund.cyber.pump

import fund.cyber.node.common.StackCache
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import kotlin.reflect.KFunction0


private val log = LoggerFactory.getLogger(ChainPumper::class.java)!!

class ChainPumper<in T : BlockBundle>(

        private val blockchainInterface: FlowableBlockchainInterface<T>,
        private val storageActionsFactories: List<StorageActionFactory> = emptyList(),

        private val storages: List<StorageInterface> = emptyList(),
        private val stateStorage: StateStorage,

        private val errorCallback: KFunction0<Unit> = PumpsContext::closeContext
) {


    fun start() {
        try {
            initializeStorages()
            initializeIndexing()
        } catch (e: Exception) {
            log.error("Error during starting '${blockchainInterface.chain}' chain pump", e)
            errorCallback()
        }
    }

    private fun initializeStorages() {
        storages.forEach { storage ->
            storage.initialize(blockchainInterface)
            registerStorageActionFactories(storage)
        }
    }

    private fun registerStorageActionFactories(storage: StorageInterface) {
        storageActionsFactories.forEach { actionFactory ->
            storage.registerStorageActionFactory(blockchainInterface.chain, actionFactory)
        }
    }

    private fun initializeIndexing() {
        val startBlockNumber = getStartBlockNumber()
        val history: StackCache<List<StorageAction>> = StackCache(20)
        initializeStreamProcessing(startBlockNumber, history)
    }

    private fun getStartBlockNumber(): Long {
        return if (PumpsConfiguration.startBlock == CS_START_BLOCK_DEFAULT)
            stateStorage.getLastCommittedState(blockchainInterface.chain) ?: 0
        else
            PumpsConfiguration.startBlock
    }

    private fun initializeStreamProcessing(startBlockNumber: Long, history: StackCache<List<StorageAction>>) {

        blockchainInterface.subscribeBlocks(startBlockNumber)
                .doOnTerminate(errorCallback)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.io())
                .scan { current, next ->
                    if (current.hash != next.parentHash) {
                        log.info("${current.chain} chain reorganisation occupied for `${current.number}` block")
                        history.pop()?.forEach(StorageAction::remove)
                        initializeStreamProcessing(current.number - 1, history)
                        throw ChainReindexationException()
                    }
                    next
                }
                .skipWhile { bundle -> bundle.number == startBlockNumber && startBlockNumber != 0L }
                .subscribe(
                        { blockBundle ->

                            log.debug("Storing ${blockBundle.chain} ${blockBundle.number} block")
                            val actions = storages.map { storage -> storage.constructAction(blockBundle) }
                            actions.forEach(StorageAction::store)
                            history.push(actions)
                            stateStorage.commitState(blockBundle)
                        },
                        { error ->
                            if (error !is ChainReindexationException)
                                log.error("Error during processing ${blockchainInterface.chain} stream", error)

                        }
                )
    }
}


class ChainReindexationException : RuntimeException()