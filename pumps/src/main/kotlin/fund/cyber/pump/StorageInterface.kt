package fund.cyber.pump

import fund.cyber.node.common.Chain

interface StorageInterface {
    fun initialize(blockchainInterface: BlockchainInterface<*>)
    fun registerStorageActionFactory(chain: Chain, actionFactory: StorageActionFactory)
    fun constructAction(blockBundle: BlockBundle): StorageAction
}

interface StateStorage {
    fun getLastCommittedState(chain: Chain): Long?
    fun commitState(blockBundle: BlockBundle)
}

interface StorageAction {
    fun store()
    fun remove()
}

object EmptyStorageAction : StorageAction {
    override fun store() {}
    override fun remove() {}
}

interface StorageActionFactory