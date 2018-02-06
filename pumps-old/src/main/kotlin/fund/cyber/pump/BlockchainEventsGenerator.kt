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
    val flowedBlockchain = ConcurrentPulledBlockchain(blockchain)
    val context = ConvertContext(
            exHash = "",
            history = StackCache(PumpsContext.stackCacheSize),
            blockByNumber = blockchain::blockBundleByNumber,
            upToDateBlockNumber = blockchain.lastNetworkBlock()
    )

    return flowedBlockchain.subscribeBlocks(startBlockNumber)
            .flatMap { blockBundle ->
                return@flatMap convert(blockBundle, context).toFlowable()
            }
}

fun convert(blockBundle: BlockBundle, context: ConvertContext): List<BlockchainEvent> {
    var events: List<BlockchainEvent>

    if (context.exHash == "" || context.exHash == blockBundle.parentHash) {
        context.exHash = blockBundle.hash
        context.history.push(blockBundle)
        events = listOf(CommitBlock(blockBundle))
    } else {
        var tempBlockBundle = blockBundle
        var prevBlockBundle: BlockBundle? = null

        var commitBlocks: List<CommitBlock> = listOf(CommitBlock(tempBlockBundle))
        var revertBlocks: List<RevertBlock> = emptyList()

        do {
            if (prevBlockBundle != null) {
                revertBlocks += RevertBlock(prevBlockBundle)
                tempBlockBundle = context.blockByNumber(tempBlockBundle.number - 1L)//exBl
                commitBlocks += CommitBlock(tempBlockBundle)
            }
            prevBlockBundle = context.history.pop()
        } while (prevBlockBundle?.hash != tempBlockBundle.parentHash)

        commitBlocks = commitBlocks.reversed()
        commitBlocks.forEach {context.history.push(it.bundle)}

        events = revertBlocks + commitBlocks
        events = listOf(ChainReorganizationBegin()) + events + ChainReorganizationEnd()
    }
    if (context.upToDateBlockNumber != null) {
        if (blockBundle.number == context.upToDateBlockNumber)
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