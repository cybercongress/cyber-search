package fund.cyber.pump

interface StorageInterface {
    fun initialize(blockchainInterfaces: List<BlockchainInterface<*>>)
    fun constructAction(blockBundle: BlockBundle): StorageAction?
}

interface StorageAction {
    fun store()
    fun remove()
}