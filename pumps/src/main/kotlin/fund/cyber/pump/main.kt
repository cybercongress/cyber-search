package fund.cyber.pump

import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.common.Chain.BITCOIN_CASH
import fund.cyber.node.common.StackCache
import fund.cyber.pump.bitcoin.BitcoinBlockchainInterface
import fund.cyber.pump.bitcoin_cash.BitcoinCashBlockchainInterface
import fund.cyber.pump.cassandra.CassandraStorage
import io.reactivex.disposables.Disposable
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory


private val log = LoggerFactory.getLogger(PumpsContext::class.java)!!


fun main(args: Array<String>) {

    val blockchainsInterfaces = PumpsConfiguration.chainsToPump.mapNotNull(::initializeBlockchainInterface)
    val storages: List<StorageInterface> = listOf(CassandraStorage())

    storages.forEach { storage ->
        storage.initialize(blockchainsInterfaces)
    }

    blockchainsInterfaces.forEach { blockchainInterface ->
        storages.forEach { storage ->
            val history: StackCache< Pair<BlockBundle, StorageAction> > = StackCache(20)
            var exDisposable: Disposable? = null

            fun act(number: Long, exHash: String) {
                exDisposable?.dispose()
                exDisposable = initFlowable(blockchainInterface, storage, number, exHash, history) { number, exHash ->
                    act(number, exHash)
                }
            }

            act(getStartBlockNumber(blockchainInterface), "")
        }
    }

    if(blockchainsInterfaces.isEmpty()) {
        PumpsContext.closeContext()
    }
}

fun initFlowable(blockhain: BlockchainInterface<*>,
                 storage: StorageInterface,
                 number: Long,
                 startExHash: String,
                 history: StackCache< Pair<BlockBundle, StorageAction> >,
                 needToContinueWith: (number: Long, exHash: String)->Unit
): Disposable {
    var exHash: String = startExHash

    return blockhain.subscribeBlocks(number)
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.computation())
            .unsubscribeOn(Schedulers.trampoline())
            .doAfterTerminate(PumpsContext::closeContext)
            .subscribe { blockBundle ->
                if (exHash == blockBundle.parentHash || blockBundle.number == 0L) {
                    val action = storage.constructAction(blockBundle)
                    history.push(Pair(blockBundle, action))
                    exHash = blockBundle.hash
                    action.store()
                } else {
                    val pair = history.pop()
                    val hAction = pair?.second
                    val hBlock = pair?.first
                    exHash = hBlock?.parentHash ?: ""
                    hAction?.remove()
                    needToContinueWith(blockBundle.number - 1, exHash)
                }
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