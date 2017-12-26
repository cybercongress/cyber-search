package fund.cyber.pump

import fund.cyber.node.common.Chain
import fund.cyber.node.model.CyberSearchItem

interface StorageInterface {
    fun initialize(blockchainInterface: BlockchainInterface<*>)
    fun setStorageActionSourceFactoryFor(chain: Chain, actionSourceFactory: StorageActionSourceFactory)
    fun constructAction(blockBundle: BlockBundle): StorageAction
}

interface EntitiesStorageInterface {
    fun constructAction(entity: CyberSearchItem, chain: Chain): StorageAction
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

interface StorageActionSourceFactory