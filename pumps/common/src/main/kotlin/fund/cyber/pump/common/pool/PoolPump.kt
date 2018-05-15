package fund.cyber.pump.common.pool

import fund.cyber.search.model.PoolItem
import fund.cyber.search.model.events.PumpEvent
import io.reactivex.Flowable
import io.reactivex.schedulers.Schedulers
import org.reactivestreams.Subscriber
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Component

private val log = LoggerFactory.getLogger(PoolPump::class.java)!!

@Component
@ConditionalOnBean(PoolInterface::class)
@DependsOn("kafkaPoolItemProducer")
class PoolPump<T : PoolItem>(
    private val poolInterface: PoolInterface<T>,
    private val poolItemProducer: KafkaPoolItemProducer
) {

    fun startPump() {
        log.debug("Starting pool pump")

        FlowableTxPool(poolInterface)
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.computation())
            .subscribe (
                { item ->
                    log.debug("New pool item received $item")
                    poolItemProducer.storeItems(listOf(PumpEvent.NEW_POOL_TX to item))
                },
                { error ->
                    log.error("Error during processing pool...", error)
                    log.info("Restarting pool pump after error")
                    startPump()
                }
            )
    }
}

class FlowableTxPool<T : PoolItem>(private val source: PoolInterface<T>) : Flowable<T>() {
    override fun subscribeActual(s: Subscriber<in T>?) {
        source.onNewItem(
            { item -> s?.onNext(item) },
            { error -> s?.onError(error) }
        )
    }
}
