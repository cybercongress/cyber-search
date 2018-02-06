package fund.cyber.pump.bitcoin.client

import cyber.search.model.bitcoin.BitcoinBlock
import cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.pump.common.BlockBundle
import fund.cyber.pump.common.BlockchainInterface


class BitcoinBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        val block: BitcoinBlock,
        val transactions: List<BitcoinTx>
) : BlockBundle

class BitcoinBlockchainInterface(
        private val bitcoinJsonRpcClient: BitcoinJsonRpcClient,
        private val rpcToBundleEntitiesConverter: JsonRpcBlockToBitcoinBundleConverter
) : BlockchainInterface<BitcoinBlockBundle> {

    override fun lastNetworkBlock(): Long = bitcoinJsonRpcClient.getLastBlockNumber()

    override fun blockBundleByNumber(number: Long): BitcoinBlockBundle {
        val block = bitcoinJsonRpcClient.getBlockByNumber(number)!!
        return rpcToBundleEntitiesConverter.convertToBundle(block)
    }
}