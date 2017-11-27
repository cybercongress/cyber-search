package fund.cyber.pump

import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.common.Chain.BITCOIN_CASH
import fund.cyber.pump.bitcoin.BitcoinBlockchainInterface
import fund.cyber.pump.bitcoin_cash.BitcoinCashBlockchainInterface
import fund.cyber.pump.cassandra.CassandraStorage
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory


private val log = LoggerFactory.getLogger(PumpsContext::class.java)!!


fun main(args: Array<String>) {

    val blockchainsInterfaces = PumpsConfiguration.chainsToPump.mapNotNull(::initializeBlockchainInterface)
    val storages: List<StorageInterface> = listOf(CassandraStorage())

    storages.forEach { storage ->
        storage.initialize(blockchainsInterfaces)
    }

    blockchainsInterfaces.forEach { blockchain ->

        val startBlockNumber = getStartBlockNumber(blockchain)
        log.info("${blockchain.chain} pump application started from block $startBlockNumber")

        blockchain.subscribeBlocks(startBlockNumber)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .unsubscribeOn(Schedulers.trampoline())
                .doAfterTerminate(PumpsContext::closeContext)
                .subscribe { blockBundle ->
                    Thread.sleep(200)
                    log.info(blockBundle.toString())
                }
    }

    if (blockchainsInterfaces.isEmpty()) {
        PumpsContext.closeContext()
    }
}


private fun getStartBlockNumber(blockchainInterface: BlockchainInterface<*>): Long {

    val applicationId = chainApplicationId(blockchainInterface.chain)

    return if (PumpsConfiguration.startBlock == CS_START_BLOCK_DEFAULT)
        PumpsContext.pumpDaoService.indexingProgressStore.get(applicationId)?.block_number ?: 0
    else
        PumpsConfiguration.startBlock
}

private fun initializeBlockchainInterface(chain: Chain): BlockchainInterface<*>? {
    return when (chain) {
        BITCOIN -> BitcoinBlockchainInterface()
        BITCOIN_CASH -> BitcoinCashBlockchainInterface()
        else -> null
    }
}