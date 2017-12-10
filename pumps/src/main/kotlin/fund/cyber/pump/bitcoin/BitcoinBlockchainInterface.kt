package fund.cyber.pump.bitcoin

import fund.cyber.cassandra.migration.Migration
import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.model.*
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.BlockchainInterface


class BitcoinBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        override val chain: Chain,
        val block: BitcoinBlock,
        val transactions: List<BitcoinTransaction>,
        val addressTransactions: List<BitcoinAddressTransaction>
) : BlockBundle {


    @Suppress("UNCHECKED_CAST")
    override fun elementsMap(): Map<Class<CyberSearchItem>, List<CyberSearchItem>> {

        val map: MutableMap<Class<CyberSearchItem>, List<CyberSearchItem>> = mutableMapOf()
        map.put(BitcoinBlock::class.java as Class<CyberSearchItem>, listOf(block))
        map.put(BitcoinTransaction::class.java as Class<CyberSearchItem>, transactions)
        map.put(BitcoinBlockTransaction::class.java as Class<CyberSearchItem>, block.transactionPreviews)
        map.put(BitcoinAddressTransaction::class.java as Class<CyberSearchItem>, addressTransactions)

        return map
    }
}

open class BitcoinBlockchainInterface(
        private val bitcoinJsonRpcClient: BitcoinJsonRpcClient = BitcoinPumpContext.bitcoinJsonRpcClient,
        private val rpcToBundleEntitiesConverter: JsonRpcBlockToBitcoinBundleConverter = BitcoinPumpContext.rpcToBundleEntitiesConverter
) : BlockchainInterface<BitcoinBlockBundle> {

    override val chain = BITCOIN
    override val migrations: List<Migration> = BitcoinMigrations.migrations


    override fun lastNetworkBlock() = bitcoinJsonRpcClient.getLastBlockNumber()

    override fun blockBundleByNumber(number: Long): BitcoinBlockBundle {
        val block = bitcoinJsonRpcClient.getBlockByNumber(number)!!
        return rpcToBundleEntitiesConverter.convertToBundle(block)
    }
}