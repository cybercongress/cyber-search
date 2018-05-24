package fund.cyber.pump.bitcoin.client


import fund.cyber.pump.bitcoin.client.converter.JsonRpcBlockToBitcoinBundleConverter
import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.pump.common.node.BlockchainInterface
import fund.cyber.pump.common.pool.PoolInterface
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.chains.ChainEntity
import fund.cyber.search.model.chains.ChainEntityType
import io.micrometer.core.instrument.MeterRegistry
import io.reactivex.Flowable
import io.reactivex.schedulers.Schedulers
import org.ehcache.Cache
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit


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

const val MEMPOOL_TXES_QUERYING_INTERVAL = 2L
const val MEMPOOL_MAX_CONCURRENCY = 16

@Component
class BitcoinBlockchainInterface(
    private val bitcoinJsonRpcClient: BitcoinJsonRpcClient,
    private val rpcToBundleEntitiesConverter: JsonRpcBlockToBitcoinBundleConverter,
    monitoring: MeterRegistry,
    private val mempoolHashesCache: Cache<String, String>
) : BlockchainInterface<BitcoinBlockBundle>, PoolInterface<BitcoinTx> {

    private val downloadSpeedMonitor = monitoring.timer("pump_bundle_download")

    private val mempoolCacheValue = ""

    override fun lastNetworkBlock(): Long = bitcoinJsonRpcClient.getLastBlockNumber()

    override fun blockBundleByNumber(number: Long): BitcoinBlockBundle {
        return downloadSpeedMonitor.recordCallable {
            val block = bitcoinJsonRpcClient.getBlockByNumber(number)!!
            return@recordCallable rpcToBundleEntitiesConverter.convertToBundle(block)
        }
    }

    override fun subscribePool(): Flowable<BitcoinTx> {
        warmUpMempoolCache()

        return Flowable
            .interval(MEMPOOL_TXES_QUERYING_INTERVAL, TimeUnit.SECONDS)
            .onBackpressureDrop()
            .map { _ ->  bitcoinJsonRpcClient.getTxMempool()}
            .flatMap({ txes ->
                Flowable
                    .fromIterable(txes)
                    .filter { hash -> mempoolHashesCache[hash] == null }
                    .flatMap({ hash ->
                        Flowable.just(hash).subscribeOn(Schedulers.io()).map(this::getMempoolTx)
                    }, MEMPOOL_MAX_CONCURRENCY)
            }, 1)
    }

    private fun warmUpMempoolCache() {
        bitcoinJsonRpcClient.getTxMempool().forEach { hash -> mempoolHashesCache.put(hash, "") }
    }

    private fun getMempoolTx(hash: String): BitcoinTx {
        mempoolHashesCache.put(hash, mempoolCacheValue)
        return rpcToBundleEntitiesConverter.convertToMempoolTx(bitcoinJsonRpcClient.getTx(hash))
    }
}
