package fund.cyber.pump

import io.reactivex.Flowable
import io.reactivex.functions.BiFunction
import io.reactivex.rxkotlin.toFlowable
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable


interface FlowableBlockchainInterface<T : BlockBundle> : BlockchainInterface<T> {
    fun subscribeBlocks(startBlockNumber: Long): Flowable<T>
}

class ConcurrentPulledBlockchain<T : BlockBundle>(
        private val blockchainInterface: BlockchainInterface<T>,
        private val batchSize: Int = 8
) : FlowableBlockchainInterface<T>, BlockchainInterface<T> by blockchainInterface {

    private val log = LoggerFactory.getLogger(ConcurrentPulledBlockchain::class.java)!!


    override fun subscribeBlocks(startBlockNumber: Long): Flowable<T> {

        var lastNetworkBlock = lastNetworkBlock()

        return Flowable.generate<LongRange, Long>(Callable { startBlockNumber }, BiFunction { nextBlockNumber, emitter ->

            val isBatchFetch = lastNetworkBlock - nextBlockNumber > batchSize

            if (!isBatchFetch) {
                lastNetworkBlock = lastNetworkBlock()
                if (nextBlockNumber == lastNetworkBlock) {
                    log.debug("Up-to-date block $nextBlockNumber for ${blockchainInterface.chain}")
                    return@BiFunction nextBlockNumber
                }
            }

            val left = nextBlockNumber
            val right = if (left + batchSize > lastNetworkBlock) lastNetworkBlock else left + batchSize

            emitter.onNext(left..right)
            return@BiFunction right + 1
        })
                .flatMap({ buffer ->
                    log.debug("Looking for ${buffer.first}-${buffer.last} blocks for ${blockchainInterface.chain}")
                    buffer.toFlowable()
                            .flatMap({ number ->
                                Flowable.just(number)
                                        .subscribeOn(Schedulers.io())
                                        .map { _ -> blockBundleByNumber(number) }
                            }, 16)
                            .sorted { o1, o2 -> o1.number.compareTo(o2.number) }
                }, 1)
    }
}