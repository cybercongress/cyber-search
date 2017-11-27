package fund.cyber.pump

interface StorageInterface {
    fun initialize(blockchainInterfaces: List<BlockchainInterface<*>>)
    fun constructAction(blockBundle: BlockBundle): StorageAction
}

interface StorageAction {
    companion object {
        val empty = EmptyStorageAction
    }

    fun store()
    fun remove()
}

object EmptyStorageAction: StorageAction {
    override fun store() {
        println("StorageAction: empty.store()")
    }

    override fun remove() {
        println("StorageAction: empty.remove()")
    }
}