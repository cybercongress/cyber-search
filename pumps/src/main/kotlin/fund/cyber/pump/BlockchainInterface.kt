package fund.cyber.pump

import fund.cyber.node.common.Chain
import io.reactivex.Flowable


interface BlockchainInterface<T> {
    val chain: Chain
    fun subscribeBlocks(startBlockNumber: Long): Flowable<T>
}