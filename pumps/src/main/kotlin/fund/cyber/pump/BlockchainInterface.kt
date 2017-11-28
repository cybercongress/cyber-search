package fund.cyber.pump

import fund.cyber.node.common.Chain
import io.reactivex.Flowable

interface BlockchainInterface<T : BlockBundle> {
    val chain: Chain
    fun subscribeBlocks(startBlockNumber: Long): Flowable<T>
}

interface BlockBundle {
    val chain: Chain
    val hash: String
    val parentHash: String
    val number: Long
}

class ChainReindexationException : RuntimeException()