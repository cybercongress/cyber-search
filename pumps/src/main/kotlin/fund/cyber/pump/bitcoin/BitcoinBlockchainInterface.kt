package fund.cyber.pump.bitcoin

import fund.cyber.dao.migration.Migratory
import fund.cyber.dao.migration.Migration
import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.model.BitcoinBlock
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.BlockchainInterface


class BitcoinBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        override val chain: Chain,
        val block: BitcoinBlock,
        val transactions: List<BitcoinTransaction>
) : BlockBundle


open class BitcoinBlockchainInterface(
        private val bitcoinJsonRpcClient: BitcoinJsonRpcClient = BitcoinPumpContext.bitcoinJsonRpcClient,
        private val rpcToBundleEntitiesConverter: JsonRpcBlockToBitcoinBundleConverter = BitcoinPumpContext.rpcToBundleEntitiesConverter
) : BlockchainInterface<BitcoinBlockBundle>, Migratory {

    override val chain = BITCOIN
    override val migrations: List<Migration> = BitcoinMigrations.migrations


    override fun lastNetworkBlock() = bitcoinJsonRpcClient.getLastBlockNumber()

    override fun blockBundleByNumber(number: Long): BitcoinBlockBundle {
        val block = bitcoinJsonRpcClient.getBlockByNumber(number)!!
        return rpcToBundleEntitiesConverter.convertToBundle(block)
    }
}