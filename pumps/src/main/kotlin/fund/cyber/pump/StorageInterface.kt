package fund.cyber.pump

interface StorageInterface {

    fun initialize(blockchainInterfaces: List<BlockchainInterface<*>>)
}

interface StorageAction {
    fun store()
    fun remove()
}