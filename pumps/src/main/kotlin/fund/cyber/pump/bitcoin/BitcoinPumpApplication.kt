package fund.cyber.pump.bitcoin

import fund.cyber.pump.PumpsContext
import fund.cyber.pump.bitcoin_cash.BitcoinCashMigrations
import fund.cyber.pump.common.TxMempool


fun main(args: Array<String>) {


    PumpsContext.schemaMigrationEngine.executeSchemaUpdate(BitcoinCashMigrations.migrations)
    initializeMempoolPumping()
}


fun initializeMempoolPumping() {

    val mempoolTxesHashes = BitcoinPumpContext.bitcoinDaoService.getMempoolTxesHashes()
    val mempool = TxMempool(mempoolTxesHashes)


    Thread().run {
        while (true) {
            println("Pooling mempool ${System.currentTimeMillis()}")

            val currentNetworkPool = BitcoinPumpContext.bitcoinJsonRpcClient.getTxMempool()
            println("Current mempool size ${currentNetworkPool.size}")

            val newTxesHashes = currentNetworkPool.filterNot(mempool::isTxIndexed)
            mempool.txesAddedToIndex(newTxesHashes)
            println("${newTxesHashes.size} new txes")

            val txes = BitcoinPumpContext.bitcoinJsonRpcClient.getTxes(newTxesHashes)
            txes.forEach(::println)

            println("Pooling mempool finished ${System.currentTimeMillis()}")
        }
    }
}