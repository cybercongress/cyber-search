package fund.cyber.pump.common

import io.reactivex.Emitter
import io.reactivex.Flowable
import io.reactivex.functions.BiFunction
import io.reactivex.rxkotlin.toFlowable
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable


interface FlowableBlockchainInterface<T : BlockBundle> : BlockchainInterface<T> {
    fun subscribeBlocks(startBlockNumber: Long): Flowable<T>
}


private val log = LoggerFactory.getLogger(ConcurrentPulledBlockchain::class.java)!!

class ConcurrentPulledBlockchain<T : BlockBundle>(
        private val blockchainInterface: BlockchainInterface<T>,
        private val batchSize: Int = 4
) : FlowableBlockchainInterface<T>, BlockchainInterface<T> by blockchainInterface {

    private var lastNetworkBlock = 0L

    // generate block number ranges, example 4000-4003, 4004-4007
    private val generateAvailableBlocksNumbersRangesFunction =
            BiFunction<Long, Emitter<LongRange>, Long> { nextBlockNumber, emitter ->

                val isBatchFetch = lastNetworkBlock - nextBlockNumber > batchSize

                if (!isBatchFetch) {
                    lastNetworkBlock = lastNetworkBlock()
                    if (nextBlockNumber == lastNetworkBlock) {
                        log.debug("Up-to-date block $nextBlockNumber")
                        return@BiFunction nextBlockNumber
                    }
                }

                val left = nextBlockNumber
                val right = if (left + batchSize > lastNetworkBlock) lastNetworkBlock else left + batchSize

                emitter.onNext(left..right)
                return@BiFunction right + 1
            }


    // 1) generate ranges of available blocks, that will be downloaded consistently
    // 2) download each group member in parallel
    override fun subscribeBlocks(startBlockNumber: Long): Flowable<T> {

        return Flowable.generate<LongRange, Long>(Callable { startBlockNumber }, generateAvailableBlocksNumbersRangesFunction)
                .flatMap({ blockNumbers -> asyncDownloadBlocks(blockNumbers) }, 1)
    }


    private fun asyncDownloadBlocks(blockNumbers: LongRange): Flowable<T> {
        log.debug("Looking for ${blockNumbers.first}-${blockNumbers.last} blocks")
        return blockNumbers.toFlowable()
                .flatMap({ number -> asyncDownloadBlock(number) }, 16)
                .sorted { o1, o2 -> o1.number.compareTo(o2.number) }
    }

    private fun asyncDownloadBlock(blockNumber: Long): Flowable<T> {
        return Flowable.just(blockNumber)
                .subscribeOn(Schedulers.io())
                .map { flowedNumber -> blockBundleByNumber(flowedNumber) }
    }
}
