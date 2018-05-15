package fund.cyber.pump.bitcoin.client


import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.pump.common.node.BlockchainInterface
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.chains.ChainEntity
import fund.cyber.search.model.chains.ChainEntityType
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component


class BitcoinBlockBundle(
    override val hash: String,
    override val parentHash: String,
    override val number: Long,
    override val blockSize: Int,
    val block: BitcoinBlock,
    val transactions: List<BitcoinTx>
) : BlockBundle {

    override fun entitiesByType(chainEntityType: ChainEntityType): List<ChainEntity> {
        return when(chainEntityType) {
            ChainEntityType.BLOCK -> listOf(block)
            ChainEntityType.TX -> transactions
            else -> emptyList()
        }
    }
}

@Component
class BitcoinBlockchainInterface(
        private val bitcoinJsonRpcClient: BitcoinJsonRpcClient,
        private val rpcToBundleEntitiesConverter: JsonRpcBlockToBitcoinBundleConverter,
        monitoring: MeterRegistry
) : BlockchainInterface<BitcoinBlockBundle> {

    private val downloadSpeedMonitor = monitoring.timer("pump_bundle_download")

    override fun lastNetworkBlock(): Long = bitcoinJsonRpcClient.getLastBlockNumber()

    override fun blockBundleByNumber(number: Long): BitcoinBlockBundle {
        return downloadSpeedMonitor.recordCallable {
            val block = bitcoinJsonRpcClient.getBlockByNumber(number)!!
            return@recordCallable rpcToBundleEntitiesConverter.convertToBundle(block)
        }
    }
}
