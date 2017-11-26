package fund.cyber.pump.bitcoin

import fund.cyber.node.model.JsonRpcBitcoinBlock
import fund.cyber.pump.PumpsContext
import fund.cyber.pump.common.TxMempool
import getStartBlockNumber
import io.reactivex.Emitter
import io.reactivex.Flowable
import io.reactivex.functions.BiFunction
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable


private val log = LoggerFactory.getLogger(BitcoinPumpContext::class.java)!!

fun main(args: Array<String>) {


    PumpsContext.schemaMigrationEngine.executeSchemaUpdate(BitcoinMigrations.migrations)
    val startBlockNumber = getStartBlockNumber(BitcoinMigrations.applicationId, PumpsContext.pumpDaoService)
    log.info("Bitcoin application started from block $startBlockNumber")



    val startBlock: Callable<Long> = Callable { 0L }

    Flowable.generate<JsonRpcBitcoinBlock, Long>(startBlock, downloadNextBlockFunction2(BitcoinPumpContext.btcdClient))
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.computation())
            .unsubscribeOn(Schedulers.trampoline())
            .doAfterTerminate(PumpsContext::closeContext)
            .subscribe { btcdBlock ->
                Thread.sleep(200)
                log.info(btcdBlock.toString())
            }

    initializeMempoolPumping()
}


fun initializeMempoolPumping() {

    val mempoolTxesHashes = BitcoinPumpContext.bitcoinDaoService.getMempoolTxesHashes()
    val mempool = TxMempool(mempoolTxesHashes)


    Thread().run {
        while (true) {
            println("Pooling mempool ${System.currentTimeMillis()}")

            val currentNetworkPool = BitcoinPumpContext.btcdClient.getTxMempool()
            println("Current mempool size ${currentNetworkPool.size}")

            val newTxesHashes = currentNetworkPool.filterNot(mempool::isTxIndexed)
            mempool.txesAddedToIndex(newTxesHashes)
            println("${newTxesHashes.size} new txes")

            val txes = BitcoinPumpContext.btcdClient.getTxes(newTxesHashes)
            txes.forEach(::println)

            println("Pooling mempool finished ${System.currentTimeMillis()}")
        }
    }
}

fun downloadNextBlockFunction2(btcdClient: BitcoinJsonRpcClient) = BiFunction { blockNumber: Long, subscriber: Emitter<JsonRpcBitcoinBlock> ->
    try {
        log.info("Pulling block $blockNumber")
        val blockHash = btcdClient.getBlockHash(blockNumber)!!
        val block = btcdClient.getBlockByHash(blockHash)
        if (block != null) {
            subscriber.onNext(block)
            blockNumber + 1
        } else blockNumber
    } catch (e: Exception) {
        log.error("error during download block $blockNumber", e)
        blockNumber
    }
}


fun downloadNextBlockFunction(btcdClient: BitcoinJsonRpcClient) = BiFunction { blockNumber: Long, subscriber: Emitter<JsonRpcBitcoinBlock> ->
    try {
        log.info("Pulling block $blockNumber")
        val block = btcdClient.getBlockByNumber(blockNumber)
        if (block != null) {
            subscriber.onNext(block)
            blockNumber + 1
        } else blockNumber
    } catch (e: Exception) {
        log.error("error during download block $blockNumber", e)
        blockNumber
    }
}