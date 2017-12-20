package fund.cyber.pump

import fund.cyber.node.common.StackCache
import fund.cyber.node.model.CyberSearchItem
import io.reactivex.Emitter
import io.reactivex.Flowable
import io.reactivex.rxkotlin.toFlowable
import io.reactivex.schedulers.Schedulers
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.slf4j.LoggerFactory


private val log = LoggerFactory.getLogger(ChainPump::class.java)!!

class EventDrivenChainPump(
        val blockchain: BlockchainInterface<*>,
        val storages: List<StorageInterface> = emptyList(),
        val stateStorage: StateStorage
) {
    fun start() {
        val startBlockNumber = this.getStartBlockNumberFor(blockchain)

        Flowable.just(blockchain)
            .subscribeOn(Schedulers.computation())
            .observeOn(Schedulers.computation())
            .generateEvents(startBlockNumber)
            .subscribe { event ->
                when (event) {
                    is CommitBlock -> this.store(event.bundle)
                    is RevertBlock -> this.remove(event.bundle)
                    is ChainReorganizationBegin -> log.info("${blockchain.chain} chain reorganisation begin")
                    is ChainReorganizationEnd -> log.info("${blockchain.chain} chain reorganisation end")
                    is UpToDate -> this.startPoolPump()
                    is Error -> Unit
                    else -> Unit
                }
            }
    }

    private fun store(blockBundle: BlockBundle) {
        log.info("Processing ${blockBundle.chain} ${blockBundle.number} block")
        this.storages.forEach {
            it.constructAction(blockBundle).store()
        }
        stateStorage.commitState(blockBundle)
    }

    private fun remove(blockBundle: BlockBundle) {
        log.info("Removing ${blockBundle.chain} ${blockBundle.number} block")
        this.storages.forEach {
            it.constructAction(blockBundle).remove()
        }
        stateStorage.commitState(blockBundle)
    }

    private fun startPoolPump() {
        if (blockchain is TxPoolInterface<*>) {
            FlowableTxPool(blockchain)
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.computation())
            .subscribe { searchItem ->

            }
        }
    }

    private fun getStartBlockNumberFor(blockchain: Blockchain): Long =
            if (PumpsConfiguration.startBlock == CS_START_BLOCK_DEFAULT)
                stateStorage.getLastCommittedState(blockchain.chain) ?: 0
            else
                PumpsConfiguration.startBlock
}

fun Flowable<BlockchainInterface<*>>.generateEvents(startBlockNumber: Long): Flowable<out BlockchainEvent> {
    return this.flatMap {blockchain ->
        fund.cyber.pump.generateEvents(blockchain, startBlockNumber)
    }
}

fun generateEvents(blockchain: BlockchainInterface<*>, startBlockNumber: Long): Flowable<out BlockchainEvent> {
    val fl = ConcurrentPulledBlockchain(blockchain)
    val context = ConvertContext(
            exHash = "",
            history = StackCache(20),
            blockByNumber = blockchain::blockBundleByNumber,
            upToDateBlockNumber = blockchain.lastNetworkBlock()
    )

    return fl.subscribeBlocks(startBlockNumber)
            .flatMap { blockBundle ->
                return@flatMap convert(blockBundle, context).toFlowable()
            }
}

fun convert(_blockBundle: BlockBundle, context: ConvertContext): List<BlockchainEvent> {
    var blockBundle = _blockBundle
    var events: List<out BlockchainEvent>

    if (context.exHash == "" || context.exHash == blockBundle.parentHash) {
        context.exHash = blockBundle.hash
        context.history.push(blockBundle)
        events = listOf(CommitBlock(blockBundle))
    } else {
        var exBl: BlockBundle? = null

        var commitBlocks: List<CommitBlock> = listOf(CommitBlock(blockBundle))
        var revertBlocks: List<RevertBlock> = emptyList()

        do {
            if (exBl != null) {
                revertBlocks += RevertBlock(exBl)
                blockBundle = context.blockByNumber(blockBundle.number - 1L)//exBl
                commitBlocks += CommitBlock(blockBundle)
            }
            exBl = context.history.pop()
        } while (exBl?.hash != blockBundle.parentHash)

        commitBlocks = commitBlocks.reversed()
        commitBlocks.forEach {context.history.push(it.bundle)}

        events = revertBlocks + commitBlocks
        events = listOf(ChainReorganizationBegin()) + events + ChainReorganizationEnd()
    }
    if (context.upToDateBlockNumber != null) {
        if (_blockBundle.number == context.upToDateBlockNumber)
            events += UpToDate()
    }
    return events
}

data class ConvertContext(
        var exHash: String = "",
        val history: StackCache<BlockBundle> = StackCache(20),
        val blockByNumber: (Long)->BlockBundle,
        val upToDateBlockNumber: Long? = null
)

interface BlockchainEvent

class Error: BlockchainEvent

data class CommitBlock(val bundle: BlockBundle): BlockchainEvent

data class RevertBlock(val bundle: BlockBundle): BlockchainEvent

class UpToDate: BlockchainEvent

class ChainReorganizationBegin: BlockchainEvent

class ChainReorganizationEnd: BlockchainEvent

class FlowablePool<T: CyberSearchItem>(source: TxPoolInterface<T>) {
    val flow: Flowable<*> = Flowable.empty<T>()
}

class ForInterfaceTesting: Publisher<CyberSearchItem> {
    override fun subscribe(s: Subscriber<in CyberSearchItem>?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

class FlowableTxPool(private val source: TxPoolInterface<out CyberSearchItem>): Flowable<CyberSearchItem>() {
    override fun subscribeActual(s: Subscriber<in CyberSearchItem>?) {
        source.onNewTransaction { tx ->
            s?.onNext(tx)
        }
    }

}


