package fund.cyber.pump

interface StorageInterface {

    fun initFor(blockchain: BlockchainInterface)

    fun store(block: Block)
}