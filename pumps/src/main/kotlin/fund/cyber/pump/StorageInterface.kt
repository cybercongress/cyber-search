package fund.cyber.pump

interface StorageInterface {

    fun initFor(blockchain: BlockchainInterface)

    fun <T>actionFor(block: T): StoreAction
}