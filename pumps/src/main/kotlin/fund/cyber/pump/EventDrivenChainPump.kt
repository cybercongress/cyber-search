package fund.cyber.pump

import fund.cyber.node.model.CyberSearchItem
import io.reactivex.Flowable
import io.reactivex.schedulers.Schedulers
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

class FlowableTxPool(private val source: TxPoolInterface<out CyberSearchItem>): Flowable<CyberSearchItem>() {
    override fun subscribeActual(s: Subscriber<in CyberSearchItem>?) {
        source.onNewTransaction { tx ->
            s?.onNext(tx)
        }
    }

}


