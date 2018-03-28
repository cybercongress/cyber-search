package fund.cyber.pump.bitcoin.client


import fund.cyber.pump.bitcoin.client.genesis.BitcoinGenesisDataProvider
import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.pump.common.node.BlockchainInterface
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinTx
import org.springframework.stereotype.Component


class BitcoinBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        override val blockSize: Int,
        val block: BitcoinBlock,
        val transactions: List<BitcoinTx>
) : BlockBundle

@Component
class BitcoinBlockchainInterface(
        private val bitcoinJsonRpcClient: BitcoinJsonRpcClient,
        private val rpcToBundleEntitiesConverter: JsonRpcBlockToBitcoinBundleConverter,
        private val genesisDataProvider: BitcoinGenesisDataProvider
) : BlockchainInterface<BitcoinBlockBundle> {

    override fun lastNetworkBlock(): Long = bitcoinJsonRpcClient.getLastBlockNumber()

    override fun blockBundleByNumber(number: Long): BitcoinBlockBundle {
        val block = bitcoinJsonRpcClient.getBlockByNumber(number)!!
        val bundle = rpcToBundleEntitiesConverter.convertToBundle(block)
        return if (number == 0L) genesisDataProvider.provide(bundle) else bundle
    }
}
