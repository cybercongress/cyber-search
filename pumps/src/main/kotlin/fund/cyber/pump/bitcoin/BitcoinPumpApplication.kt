package fund.cyber.pump.bitcoin

import fund.cyber.node.common.env
import fund.cyber.node.model.BtcdBlock
import fund.cyber.pump.PumpConfiguration
import fund.cyber.pump.PumpContext
import fund.cyber.pump.PumpsApplications.BITCOIN
import getStartBlockNumber
import io.reactivex.Emitter
import io.reactivex.Flowable
import io.reactivex.functions.BiFunction
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable


class BitcoinPumpConfiguration : PumpConfiguration() {
    val btcdUrl: String = env("BTCD_URL", "http://cyber:cyber@127.0.0.1:8334")
}

class BitcoinPumpContext : PumpContext<BitcoinPumpConfiguration>(BitcoinPumpConfiguration()) {

    val btcdClient: BtcdClient = BtcdClient(
            jacksonJsonSerializer, jacksonJsonDeserializer, httpClient, configuration.btcdUrl
    )
}


private val log = LoggerFactory.getLogger(BitcoinPumpContext::class.java)!!

fun main(args: Array<String>) {

    val appContext = BitcoinPumpContext()

    appContext.schemaMigrationEngine.executeSchemaUpdate(BitcoinMigrations.migrations)
    val startBlockNumber = getStartBlockNumber(BITCOIN, appContext.pumpDaoService, appContext.configuration)
    log.info("Bitcoin application started from block $startBlockNumber")


    val startBlock: Callable<Long> = Callable { 0L }

    Flowable.generate<BtcdBlock, Long>(startBlock, downloadNextBlockFunction(appContext.btcdClient))
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.computation())
            .unsubscribeOn(Schedulers.trampoline())
            .doAfterTerminate(appContext::closeContext)
            .subscribe { btcdBlock ->
                Thread.sleep(200)
                log.info(btcdBlock.toString())
            }
}

fun downloadNextBlockFunction(btcdClient: BtcdClient) = BiFunction { blockNumber: Long, subscriber: Emitter<BtcdBlock> ->
    try {
        log.info("Pulling blocks")
        if (blockNumber > 200) {
            subscriber.onComplete()
        }
        val block = btcdClient.getBlockByNumber(blockNumber)
        if (block != null) {
            subscriber.onNext(block)
        }
        blockNumber + 1
    } catch (e: Exception) {
        blockNumber
    }
}
