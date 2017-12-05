package fund.cyber.pump

import fund.cyber.dao.migration.Migration
import fund.cyber.node.common.Chain
import io.reactivex.Emitter
import io.reactivex.Flowable
import io.reactivex.rxkotlin.toFlowable
import io.reactivex.schedulers.Schedulers
import java.util.concurrent.Callable
import io.reactivex.functions.BiFunction


interface  FlowableBlockchain : Blockchain {
    fun subscribeBlocks(startBlockNumber: Long): Flowable<out BlockBundle>
}

abstract class AbstractFlowableBlockchain(private val blockchain: BlockchainInterface) : FlowableBlockchain, Migratory {
    override val migrations: List<Migration>
        get() = if (blockchain is Migratory)
            blockchain.migrations
        else
            emptyList()

    override val chain: Chain
        get() = blockchain.chain
}

class SerialPulledBlockchain(val blockchain: BlockchainInterface) : AbstractFlowableBlockchain(blockchain) {
    override fun subscribeBlocks(startBlockNumber: Long): Flowable<BlockBundle> {
        return Flowable.generate<BlockBundle, Long>(Callable { startBlockNumber }, downloadNextBlockFunction())

    }

    fun downloadNextBlockFunction() =
            BiFunction { blockNumber: Long, subscriber: Emitter<BlockBundle> ->
                try {
                    println(blockNumber)
                    if (blockNumber <= blockchain.lastNetowrkBlock) {
                        val block = blockchain.blockBundleByNumber(blockNumber)
                        subscriber.onNext(block)
                        return@BiFunction blockNumber + 1
                    }
                } catch (e: Exception) {
                    println("error during download block $blockNumber")
                    println(e.localizedMessage)
                }
                return@BiFunction blockNumber
            }
}

class ConcurrentPulledBlockchain(val blockchain: BlockchainInterface, val batchSize: Int = 8) : AbstractFlowableBlockchain(blockchain) {
    override fun subscribeBlocks(startBlockNumber: Long): Flowable<out BlockBundle> {
        Schedulers.computation()
        return Flowable.generate<LongRange, Long>(Callable { startBlockNumber }, BiFunction { left, emitter ->
            val lastNetworkBlock = blockchain.lastNetowrkBlock
            var right = left + batchSize

            if (right > lastNetworkBlock)
                right = lastNetworkBlock

            if (left <= right) {
                emitter.onNext(left..right)
                return@BiFunction right + 1
            }
            return@BiFunction left
        })
                .flatMap({ buffer ->
                    buffer.toFlowable()
                            .flatMap ({ number ->
                                Flowable.just(number)
                                        .subscribeOn(Schedulers.io())
                                        .map { number -> blockchain.blockBundleByNumber(number) }
                            }, 16)
                            .sorted { o1, o2 ->
                                o1.number.compareTo(o2.number)
                            }
                }, 1)
    }
}