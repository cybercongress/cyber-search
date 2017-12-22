package fund.cyber.pump
import io.reactivex.Flowable
import io.reactivex.rxkotlin.toFlowable
import fund.cyber.node.common.StackCache

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